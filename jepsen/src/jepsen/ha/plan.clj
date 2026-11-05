(ns jepsen.ha.plan
  "Switchover plan: an Euler tour over the profile's directed transitions,
  emitted one :switch op at a time by a self-contained Jepsen generator."
  (:require [jepsen.generator :as generator]
            [jepsen.ha.fault :as fault]
            [jepsen.util :as util]))

(def retryable-errors
  "Switch errors that leave the cluster on the source leader, so the same
  transition may be attempted again."
  #{:transition-returned-to-source :switchover-command-failed :candidate-not-ready})

(defn euler-tour
  "Order each configured directed transition once and return to `start`."
  [transitions start]
  (let [by-edge (into {} (map (juxt (juxt :from :candidate) identity) transitions))
        edges (set (keys by-edge))
        ordered-nodes (sort-by name (set (mapcat identity edges)))]
    (letfn [(walk [current remaining path]
              (if (empty? remaining)
                (when (= current start) path)
                (some (fn [candidate]
                        (let [edge [current candidate]]
                          (when (contains? remaining edge)
                            (walk candidate
                                  (disj remaining edge)
                                  (conj path (get by-edge edge))))))
                      ordered-nodes)))]
      (when (and (= (count edges) (count transitions))
                 (= (frequencies (map first edges)) (frequencies (map second edges))))
        (walk start edges [])))))

(defn check-tour!
  "The Euler tour of TRANSITIONS from START; throws when there is none."
  [transitions start]
  (or (euler-tour transitions start)
      (throw (ex-info "Transitions do not form an Euler cycle from the leader"
                      {:start start :transitions transitions}))))

(defn- partition-event
  "Partition state change carried by a nemesis completion: :start once the
  grudge is applied, :stop once the network is healed. Invocations are ignored."
  [event]
  (cond (fault/applied? event) :start
        (fault/healed? event) :stop))

(defn- switch-result? [event]
  (and (= :switch (:f event))
       (contains? #{:ok :info :fail} (:type event))))

(defrecord SwitchPlan [tour index attempt max-attempts pause-nanos not-before
                       in-flight? fault-active? awaiting-heal?]
  generator/Generator
  (op [this _test context]
    (cond
      ;; Done, but not while a fault is applied: the phase would end and
      ;; nobody would observe the cluster until the fault is healed.
      (= index (count tour)) (when fault-active? [:pending this])
      (or in-flight? awaiting-heal?) [:pending this]
      :else
      (let [{:keys [from candidate]} (nth tour index)
            op (generator/fill-in-op
                 {:type :invoke
                  :f :switch
                  :time (max (:time context) not-before)
                  :value {:from from
                          :candidate candidate
                          :attempt attempt
                          :index index}}
                 context)]
        (if (= :pending op)
          [:pending this]
          [op (assoc this :in-flight? true)]))))
  (update [this _test _context event]
    (cond
      (switch-result? event)
      (let [this (assoc this :in-flight? false)]
        (cond
          (= :ok (:type event))
          (assoc this
                 :index (inc index)
                 :attempt 1
                 :not-before (+ (:time event) pause-nanos))

          (and (contains? retryable-errors (get-in event [:value :error]))
               (< attempt max-attempts))
          (assoc this :attempt (inc attempt) :awaiting-heal? fault-active?)

          :else
          (assoc this :index (count tour))))

      :else
      (case (partition-event event)
        :start (assoc this :fault-active? true)
        :stop (assoc this :fault-active? false :awaiting-heal? false)
        this))))

(defn switch-plan
  "Generator walking the Euler tour of `transitions` from leader `start`, one
  :switch op in flight at a time. A retryable failure is retried as a new op
  with :attempt + 1, up to `max-attempts`; if a partition is active at that
  moment the retry waits for the nemesis :stop. Exhausted attempts or a
  non-retryable error end the plan; the history is the only record. Successful
  transitions are spaced by `pause-seconds`. The plan does not finish while a
  partition is applied: it stays pending until the heal."
  [{:keys [transitions start max-attempts pause-seconds]}]
  (let [tour (check-tour! transitions start)]
    (map->SwitchPlan {:tour (vec tour)
                      :index 0
                      :attempt 1
                      :max-attempts max-attempts
                      :pause-nanos (long (util/secs->nanos pause-seconds))
                      :not-before 0
                      :in-flight? false
                      :fault-active? false
                      :awaiting-heal? false})))
