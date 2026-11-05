(ns jepsen.ha.checker
  "Single HA checker derived from the Jepsen history alone: every switchover
  must settle on its candidate's expected topology, and after every fault
  heals the cluster must settle on some valid topology in time."
  (:require [clojure.string :as string]
            [clojure.tools.logging :refer [info]]
            [jepsen.checker :as checker]
            [jepsen.ha.fault :as fault]
            [jepsen.ha.profile :as profile]
            [jepsen.util :as util]))

(defn snapshot-matches-topology?
  "True when every profile node was observed, reachable, and every observer
  (and every member it reports) agrees with the expected topology: same
  leader, same postgres roles, the leader as the only HA leader."
  [{:keys [leader members] :as expected} observations]
  (let [role-ok? (fn [{:keys [node postgres-role ha-role]}]
                   (and (= postgres-role (get-in expected [:nodes node :postgres-role]))
                        (if (= node leader)
                          (= :leader ha-role)
                          (not (contains? #{:leader :unknown} ha-role)))))
        same-nodes? (fn [xs]
                      (and (= (count members) (count xs))
                           (= members (set (map :node xs)))))]
    (and (same-nodes? observations)
         (every? (fn [obs]
                   (and (true? (:reachable? obs))
                        (= leader (:leader obs))
                        (role-ok? obs)
                        (same-nodes? (:members obs))
                        (every? role-ok? (:members obs))))
                 observations))))

(defn- stable-runs
  "[op run] for each observe op: how many consecutive ops up to and including
  it settled on the same non-nil `(settled op)`."
  [settled observes]
  (map vector
       observes
       (map first (rest (reductions (fn [[run previous] op]
                                      (let [s (settled op)]
                                        [(cond (nil? s) 0 (= s previous) (inc run) :else 1) s]))
                                    [0 nil]
                                    observes)))))

(defn- first-stable-op
  "The first op that completes a run of `required` settled ops, or nil."
  [required settled observes]
  (some (fn [[op run]] (when (>= run required) op))
        (stable-runs settled observes)))

(defn- elapsed-ms
  "Milliseconds between the history times FROM and TO (ns); nil without TO."
  [from to]
  (when to (double (util/nanos->ms (- to from)))))

(defn- settled-leader-fn
  "Fn of an :observe op returning the leader whose expected topology the
  snapshot matches, or nil."
  [profile]
  (let [topologies (into {}
                         (keep (fn [{:keys [id]}]
                                 (try [id (profile/expected-topology profile id)]
                                      (catch clojure.lang.ExceptionInfo _ nil))))
                         (:nodes profile))]
    (fn [op]
      (let [observations (get-in op [:value :observations])
            leader (:leader (first observations))]
        (when-let [expected (get topologies leader)]
          (when (snapshot-matches-topology? expected observations)
            leader))))))

(defn- positions
  "[index op] for each op of HISTORY that satisfies PRED."
  [pred history]
  (keep-indexed (fn [i op] (when (pred op) [i op])) history))

(defn- ops-between
  "The ops of INDEXED-OPS ([index op] pairs) whose index is strictly between
  FROM and TO."
  [indexed-ops from to]
  (keep (fn [[i op]] (when (< from i to) op)) indexed-ops))

(defn- fault-spans
  "[from to] history times for each applied fault: from its nemesis completion
  to the next heal (open-ended when never healed)."
  [history]
  (let [nemesis (filter #(= :nemesis (:process %)) history)
        heals (filter fault/healed? nemesis)]
    (for [start (filter fault/applied? nemesis)]
      [(:time start)
       (or (some #(when (> (:time %) (:time start)) (:time %)) heals) Long/MAX_VALUE)])))

(defn- switchovers
  "One result per logical transition (:index), judged on its final attempt:
  it must complete :ok, settle on the target (a stable run after the command),
  and the snapshots between its invocation and the next :switch invocation (or
  the end of history) must not leave it afterwards: every calm snapshot after
  the settling is on the target, or the window ends in a stable target run.
  CALM is the subset of OBSERVES taken while no fault is applied; only that
  subset must end in a stable target run, because an unreachable node spoils
  the snapshots and the faults' own check covers that time. The timings use all
  of OBSERVES.
  :command-ms is the switchover command's own duration from the completion;
  first-target and stable times count from the command start (the invocation
  when the command never ran) and use only snapshots taken after it."
  [settled-leader conv history observes calm]
  (let [invokes (positions #(and (= :switch (:f %)) (= :invoke (:type %))) history)
        completions (filter #(and (= :switch (:f %)) (not= :invoke (:type %))) history)]
    (for [[index attempts] (sort-by key (group-by #(get-in % [1 :value :index]) invokes))
          :let [[i {:keys [value time]}] (last attempts)
                request-time (:time (second (first attempts)))
                {:keys [from candidate attempt]} value
                end (or (some (fn [[j]] (when (> j i) j)) invokes) (count history))
                window (ops-between observes i end)
                calm-window (ops-between calm i end)
                completion (some #(when (= [index attempt]
                                           ((juxt :index :attempt) (:value %)))
                                    %)
                                 completions)
                settled #(#{candidate} (settled-leader %))
                start (or (get-in completion [:value :command-start]) time)
                after-start (filter #(>= (:time %) start) window)
                first-target-time (:time (some #(when (settled %) %) after-start))
                stable-time (:time (first-stable-op (:stable-samples conv) settled after-start))
                calm-after-stable (filter #(and stable-time (>= (:time %) stable-time))
                                          calm-window)]]
      (merge
        {:index index
         :from from
         :candidate candidate
         :attempts (count attempts)
         :request-time request-time
         :command-start-time start
         :first-target-time first-target-time
         :stable-time stable-time
         :result-time (:time completion)
         :type (:type completion)
         :valid? (boolean
                   (and (= :ok (:type completion))
                        stable-time
                        (or (every? settled calm-after-stable)
                            (>= (or (second (last (stable-runs settled calm-window))) 0)
                                (:stable-samples conv)))))
         :command-ms (get-in completion [:value :command-ms])
         :first-target-ms (elapsed-ms start first-target-time)
         :stable-ms (elapsed-ms start stable-time)}
        (select-keys (:value completion) [:result :error :message])))))

(defn- incomplete-transitions
  "An error for every planned transition index whose final attempt did not
  complete :ok (never run, cut off, failed or indeterminate), and for every
  :switch index outside the plan."
  [profile switches]
  (let [planned (set (range (count (profile/plan-transitions profile))))
        by-index (into {} (map (juxt :index identity)) switches)]
    (for [index (sort (into planned (keys by-index)))
          :let [{:keys [type error message]} (get by-index index)]
          :when (or (not= :ok type) (not (planned index)))]
      {:error (if (planned index) :transition-incomplete :transition-unplanned)
       :index index
       :final-type type
       :final-error error
       :message message})))

(defn- faults
  "Each applied fault paired with the next heal; recovered when a valid
  topology is stable within the convergence timeout of the heal and before the
  next fault :start. :start and :stop are the nemesis completion times
  (history nanoseconds)."
  [settled-leader conv history observes]
  (let [nemesis (positions #(= :nemesis (:process %)) history)
        starts (filter (fn [[_ op]] (fault/applied? op)) nemesis)
        stops (filter (fn [[_ op]] (fault/healed? op)) nemesis)
        next-start (fn [j] (or (some (fn [[k op]] (when (and (> k j) (= :start (:f op))) k))
                                     nemesis)
                               (count history)))]
    (for [[i start] starts
          :let [[j stop] (some #(when (> (first %) i) %) stops)
                stable-op (when stop
                            (first-stable-op (:stable-samples conv)
                                             settled-leader
                                             (ops-between observes j (next-start j))))
                recovery-ms (when stop (elapsed-ms (:time stop) (:time stable-op)))]]
      {:pattern (:pattern start)
       :start (:time start)
       :stop (:time stop)
       :recovered? (boolean (and recovery-ms
                                 (<= recovery-ms (:timeout-ms conv))))
       :recovery-ms recovery-ms})))

(defn- ms
  "X milliseconds as \"3912ms\", or \"-\" without a value."
  [x]
  (if x (str (Math/round (double x)) "ms") "-"))

(defn summary
  "Human-readable lines about the switchovers, faults and errors in a result."
  [{:keys [switchovers faults errors]}]
  (concat
    (for [{:keys [index from candidate attempts type error command-ms first-target-ms stable-ms]}
          switchovers]
      (format "switch #%d %s -> %s: %s, %d attempt(s), command %s, target %s, stable %s"
              index from candidate (name (or error type :incomplete)) attempts
              (ms command-ms) (ms first-target-ms) (ms stable-ms)))
    (for [{:keys [pattern recovered? recovery-ms]} faults]
      (format "fault %s: %s, recovery %s"
              (some-> pattern name) (if recovered? "recovered" "NOT recovered")
              (ms recovery-ms)))
    (for [{:keys [error index message]} errors]
      (format "error %s%s%s" (some-> error name)
              (if index (str " (switch #" index ")") "")
              (if message (str ": " message) "")))))

(defn ha-checker
  "Checker over :switch, :observe and fault nemesis ops. Valid when every
  planned transition's final attempt completed :ok and settled, and every
  fault recovered; :unknown when the history has neither switchovers nor
  observations."
  [profile]
  (reify checker/Checker
    (check [_ _test history _opts]
      (let [history (vec history)
            settled-leader (settled-leader-fn profile)
            observes (positions #(and (= :observe (:f %)) (= :ok (:type %))) history)
            spans (fault-spans history)
            calm (remove (fn [[_ op]] (some (fn [[from to]] (<= from (:time op) to)) spans))
                         observes)
            switches (vec (switchovers settled-leader (:convergence profile) history observes calm))
            parts (vec (faults settled-leader (:convergence profile) history observes))
            errors (into (if (and (seq switches) (empty? observes))
                           [{:error :no-observations
                             :message "switchovers present but no :observe snapshots"}]
                           [])
                         (incomplete-transitions profile switches))
            result {:valid? (if (and (empty? switches) (empty? observes))
                              :unknown
                              (and (empty? errors)
                                   (every? :valid? switches)
                                   (every? :recovered? parts)))
                    :switchovers switches
                    :faults parts
                    :errors errors}]
        (info (str "HA summary (valid? " (:valid? result) ")\n  "
                   (string/join "\n  " (summary result))))
        result))))
