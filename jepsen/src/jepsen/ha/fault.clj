(ns jepsen.ha.fault
  "Network partitions injected during the run: the stock Jepsen partitioner
  with a randomly chosen grudge pattern. One :start op applies a partition,
  one :stop op heals it."
  (:require [clojure.tools.logging :refer [info]]
            [jepsen.generator :as generator]
            [jepsen.nemesis :as nemesis]))

(def grudges
  "Grudge functions behind Jepsen's partition-random-node, -random-halves and
  -majorities-ring nemeses; each maps a node list to a grudge."
  {:random-node (comp nemesis/complete-grudge nemesis/split-one)
   :random-halves (comp nemesis/complete-grudge nemesis/bisect shuffle)
   :majorities-ring nemesis/majorities-ring})

(defn start
  "One :start op whose grudge is computed from the pattern when it is emitted.
  The pattern name stays on the op for the checker."
  [pattern]
  (generator/once
    (fn [test _context]
      {:type :info
       :f :start
       :pattern pattern
       :value ((grudges pattern) (:nodes test))})))

(def stop
  "One :stop op that heals the network."
  (generator/once {:type :info :f :stop :value nil}))

(defn applied?
  "True for the nemesis completion of a :start op: the grudge is in place."
  [event]
  (and (= :nemesis (:process event))
       (= :start (:f event))
       (sequential? (:value event))
       (= :isolated (first (:value event)))))

(defn healed?
  "True for the nemesis completion of a :stop op: the network is healed."
  [event]
  (and (= :nemesis (:process event))
       (= :stop (:f event))
       (= :network-healed (:value event))))

(defn logged
  "Wrap NEM so that each applied partition and each heal is logged in one line."
  [nem]
  (reify nemesis/Nemesis
    (setup! [_ test] (logged (nemesis/setup! nem test)))
    (invoke! [_ test op]
      (let [done (nemesis/invoke! nem test op)]
        (cond (applied? done) (info "partition applied:" (name (:pattern op))
                                    (pr-str (second (:value done))))
              (healed? done) (info "partition healed"))
        done))
    (teardown! [_ test] (nemesis/teardown! nem test))))

(defn cycles
  "Nemesis generator for CYCLES partitions: sleep a random 1-2x
  INTERVAL-SECONDS, partition with a random pattern, hold for
  DURATION-SECONDS, heal."
  [cycles duration-seconds interval-seconds]
  (let [patterns (vec (keys grudges))]
    (mapcat (fn [_]
              [(generator/sleep (+ interval-seconds (* interval-seconds (rand))))
               (start (rand-nth patterns))
               (generator/sleep duration-seconds)
               stop])
            (range cycles))))
