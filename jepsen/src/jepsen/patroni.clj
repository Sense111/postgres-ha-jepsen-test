(ns jepsen.patroni
  "Jepsen test for Patroni: planned switchovers under network partitions,
  checked against the observed topology."
  (:require [clojure.tools.logging :refer [info]]
            [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]
                    [nemesis :as nemesis]
                    [tests :as tests]]
            [jepsen.cli :as cli]
            [jepsen.util :as util]
            [jepsen.ha [checker :as ha-checker]
                       [fault :as fault]
                       [patroni-system :as patroni]
                       [plan :as plan]
                       [profile :as profile]]))

(def threads
  "The client workers by role; Jepsen identifies workers by number. The test
  runs exactly these: one only observes, the other only switches."
  {:observer 0 :switcher 1})


(defn- poll
  "Call (step state), starting from INIT, every :poll-interval-ms of CONV (the
  profile's convergence settings) until it returns {:done x} or :timeout-ms
  passes. A step that is not done returns {:state next-state}; on timeout
  {:timeout last-state} is returned. The deadline is checked between steps, so
  a slow step can overshoot it by one step (covered by the per-attempt slack in
  time-limit-seconds)."
  [conv init step]
  (let [deadline (+ (System/nanoTime) (util/ms->nanos (:timeout-ms conv)))]
    (loop [state init]
      (let [result (step state)]
        (cond (contains? result :done) result
              (<= deadline (System/nanoTime)) {:timeout (:state result)}
              :else (do (Thread/sleep (long (:poll-interval-ms conv)))
                        (recur (:state result))))))))

(defn- observe-all
  "One snapshot of the cluster: every node's own view (patroni/observe), asked
  in parallel so an unreachable node does not delay the others."
  [system test]
  (vec (pmap #(patroni/observe system test %) (patroni/node-ids test))))

(defn- await-stable
  "Poll snapshots of all nodes until (classify snapshot) returns the same
  non-nil value for :stable-samples snapshots in a row."
  [system test conv classify]
  (poll conv [nil 0]
        (fn [[previous run]]
          (let [c (classify (observe-all system test))
                run (cond (nil? c) 0 (= c previous) (inc run) :else 1)]
            (if (>= run (:stable-samples conv))
              {:done c}
              {:state [c run]})))))

(defn- sole-leader
  "The leader every reachable node reports, when they agree on one."
  [observations]
  (let [leaders (set (map :leader (filter :reachable? observations)))]
    (when (= 1 (count leaders)) (first leaders))))

(defn- switch!
  "Run one planned transition: wait until the candidate is ready, issue the
  switchover, then wait until the target or the source topology is stable.
  The command's start (relative ns) and duration go into the value as
  :command-start and :command-ms. A command that failed or timed out still
  counts when the candidate is the leader afterwards. Only :ok advances the
  plan; the errors in plan/retryable-errors (:candidate-not-ready,
  :switchover-command-failed, :transition-returned-to-source) let it retry."
  [system profile test {{:keys [from candidate] :as transition} :value :as op}]
  (let [conv (:convergence profile)
        complete (fn [type result]
                   (info (format "switch #%d %s -> %s, attempt %d: %s"
                                 (:index transition) from candidate (:attempt transition)
                                 (if (= :ok type) "ok" (name (:error result)))))
                   (assoc op :type type :value (merge transition result)))
        readiness (poll conv nil (fn [_]
                              (let [r (patroni/ready? system test transition)]
                                (if (:ready? r) {:done r} {:state r}))))]
    (if-not (:done readiness)
      (complete :info {:error :candidate-not-ready :readiness (:timeout readiness)})
      (let [start (util/relative-time-nanos)
            result (assoc (patroni/switchover! system test transition)
                          :command-start start
                          :command-ms (/ (- (util/relative-time-nanos) start) 1e6))
            target (profile/expected-topology profile candidate)
            source (profile/expected-topology profile from)]
        (if (and (:error result)
                 (not= candidate (sole-leader (observe-all system test))))
          (complete :info result)
          ;; A command that failed or timed out may still have switched the
          ;; leader; if the candidate leads, judge the topology as usual.
          (case (:done (await-stable
                         system test conv
                         #(cond (ha-checker/snapshot-matches-topology? target %) :target
                                (ha-checker/snapshot-matches-topology? source %) :source)))
            :target (complete :ok (dissoc result :error))
            :source (complete :info (assoc result :error :transition-returned-to-source))
            (complete :info (assoc result :error :convergence-timeout))))))))

(defn client
  "Client for :observe (snapshot of every node), :leader (the initial stable
  leader) and :switch (one planned transition) ops."
  [system profile]
  (reify client/Client
    (open! [this _test _node] this)
    (setup! [_ _test])
    (invoke! [_ test op]
      (case (:f op)
        :observe (assoc op :type :ok :value {:observations (observe-all system test)})
        :leader (if-let [leader (:done (await-stable system test (:convergence profile) sole-leader))]
                  (assoc op :type :ok :value leader)
                  (assoc op :type :fail :error :no-stable-leader))
        :switch (switch! system profile test op)))
    (teardown! [_ _test])
    (close! [_ _test])
    client/Reusable
    (reusable? [_ _test] true)))

;; The main phase of `generator`
(defrecord MainPhase [plan-opts plan others]
  gen/Generator
  (op [this test ctx]
    (let [switcher-ctx (gen/on-threads-context #{(threads :switcher)} ctx)]
      (case plan
        nil (let [op (gen/fill-in-op {:type :invoke :f :leader :value nil} switcher-ctx)]
              (if (= :pending op)
                [:pending this]
                [op (assoc this :plan :awaiting-leader)]))
        :awaiting-leader [:pending this]
        (when-let [[plan-op plan'] (gen/op plan test switcher-ctx)]
          (let [[other-op others'] (gen/op others test ctx)
                {:keys [op gen]} (gen/soonest-op-map
                                   {:op plan-op :gen (assoc this :plan plan')}
                                   (when other-op
                                     {:op other-op :gen (assoc this :others others')}))]
            [op gen])))))
  (update [this test ctx event]
    (cond
      (and (= :leader (:f event)) (not= :invoke (:type event)))
      (if (= :ok (:type event))
        (assoc this :plan (plan/switch-plan (assoc plan-opts :start (:value event))))
        (throw (ex-info (str "Initial leader discovery failed: "
                             (pr-str (or (:error event) (:exception event))))
                        {:op event})))

      (keyword? plan) this

      :else (assoc this
                   :plan (gen/update plan test ctx event)
                   :others (gen/update others test ctx event)))))

(def ^:private command-allowance-seconds
  "Time reserved for the switchover command itself in every attempt of the
  main-phase limit (patronictl is killed after 10 s by default)."
  30)

(defn- time-limit-seconds
  "Generous bound on the main phase: initial leader discovery; for every
  attempt of every transition, the readiness and convergence waits (up to the
  convergence timeout each), the command allowance and a wait for a partition
  to heal; the pauses between transitions."
  [profile]
  (let [timeout (/ (get-in profile [:convergence :timeout-ms]) 1000)
        duration (get-in profile [:nemesis :duration-seconds] 0)
        transitions (count (profile/plan-transitions profile))
        attempts (get-in profile [:switchover :max-attempts])
        pause (get-in profile [:switchover :pause-seconds])]
    (+ timeout
       (* transitions attempts (+ (* 2 timeout) command-allowance-seconds duration))
       (* transitions pause))))

(defn generator
  "Main phase (MainPhase): asks the client for the initial leader, then runs
  the switch plan built from it on one client thread, :observe ops on the
  other and partition cycles on the nemesis. The plan's ops go to the switcher
  thread only, but the plan is updated with every event, nemesis ones
  included, so it can hold retries until a partition heals. The phase ends
  with the plan, or its time limit, or, when the plan finishes in the middle
  of a partition, once that has healed: the next phase only starts when the
  nemesis is idle, and until then nobody would be observing the cluster.
  Then heal the network and keep observing for the convergence timeout. Each
  phase starts once every worker is free, so the whole run is bounded by the
  main-phase limit, the ops still in flight (client polls are deadline-bound;
  a nemesis sleep lasts at most max(duration, 2 * interval)) and the
  convergence phase."
  [profile]
  (let [{:keys [cycles duration-seconds interval-seconds] :as nemesis-config}
        (:nemesis profile)
        plan-opts {:transitions (profile/plan-transitions profile)
                   :max-attempts (get-in profile [:switchover :max-attempts])
                   :pause-seconds (get-in profile [:switchover :pause-seconds])}
        {:keys [poll-interval-ms timeout-ms]} (:convergence profile)
        observe (gen/stagger (/ poll-interval-ms 1000)
                             (gen/repeat {:type :invoke :f :observe :value nil}))]
    (gen/phases
      (gen/time-limit
        (time-limit-seconds profile)
        (map->MainPhase
          {:plan-opts plan-opts
           :others (apply gen/any
                          (cond-> [(gen/on-threads #{(threads :observer)} observe)]
                            nemesis-config
                            (conj (gen/nemesis
                                    (fault/cycles cycles duration-seconds interval-seconds)))))}))
      (gen/nemesis fault/stop)
      (gen/time-limit (/ timeout-ms 1000) (gen/clients observe)))))

(defn- node-config
  "The per-node settings from the profile's `patroni` section, plus each
  node's :ssh-host: {id {:rest-api url :ssh-host host}}."
  [profile]
  (let [settings (patroni/node-config profile)]
    (into (sorted-map)
          (map (fn [{:keys [id ssh-host]}]
                 [id (assoc (settings id) :ssh-host ssh-host)]))
          (:nodes profile))))

(defn- patroni-system
  "The Patroni cluster access with the credentials from the environment."
  [profile]
  (patroni/patroni-system
    {:restapi-username (System/getenv "PATRONI_RESTAPI_USERNAME")
     :restapi-password (System/getenv "PATRONI_RESTAPI_PASSWORD")
     :command-timeout-seconds (get-in profile [:patroni :command-timeout-seconds]
                                      patroni/default-command-timeout-seconds)}))

(defn patroni-test
  "Jepsen test map for the cluster described by PROFILE, on top of the CLI
  OPTIONS. The cluster is provisioned outside Jepsen, so the DB, OS, network
  and remote are the stock defaults of tests/noop-test."
  ([profile]
   (patroni-test profile {}))
  ([profile options]
   (let [system (patroni-system profile)]
     (merge tests/noop-test
            options
            {:name "patroni"
             :nodes (mapv :ssh-host (:nodes profile))
             :ha-node-config (node-config profile)
             :concurrency (count threads)
             :logging {:overrides {"jepsen.util" :warn}} ; per-op lines; history.edn has them
             :client (client system profile)
             :nemesis (fault/logged (nemesis/partitioner))
             :generator (generator profile)
             :checker (checker/compose
                        {:ha (ha-checker/ha-checker profile)
                         :stats (checker/stats)
                         :exceptions (checker/unhandled-exceptions)})}))))

(defn- build-test
  "Load the profile and build the test; profile and plan errors keep their
  data but get a message saying what could not be built."
  [options]
  (try
    (patroni-test (profile/load-profile) options)
    (catch clojure.lang.ExceptionInfo e
      (throw (ex-info (str "Cannot build the Patroni test: " (.getMessage e))
                      (ex-data e)
                      e)))))

(defn -main
  [& args]
  (cli/run! (cli/single-test-cmd {:test-fn build-test}) args))
