(ns jepsen.ha.profile
  "Loading and validating user-editable HA test profiles. Topologies are
  derived from the node list: any node may be the leader, and the others are
  replicas that see every node. The `patroni` section holds the Patroni
  settings; the profile passes it on untouched and jepsen.ha.patroni-system
  validates it."
  (:require [clj-yaml.core :as yaml]
            [clojure.java.io :as io]
            [clojure.string :as string]
            [jepsen.ha.plan :as plan]))

(def default-profile-path "profiles/default.yaml")

(defn- fail [path message data]
  (throw (ex-info (str "Invalid HA profile " path ": " message)
                  (assoc data :profile path))))

(defn- nonblank-string? [value]
  (and (string? value) (not (string/blank? value))))

(defn- check-positive-int! [path label k value]
  (when-not (and (integer? value) (pos? value))
    (fail path (str "`" label "` must be a positive integer") {k value})))

(defn- check-map! [path label value]
  (when-not (map? value)
    (fail path (str "`" label "` must be a mapping") {:value value})))

(defn- validate-node! [path node]
  (when-not (map? node)
    (fail path "each entry in `nodes` must be a mapping" {:node node}))
  (doseq [field [:id :ssh-host]]
    (when-not (nonblank-string? (get node field))
      (fail path (str "each node must have a non-empty `" (name field) "`")
            {:node node})))
  node)

(def default-convergence
  "How the harness waits for the cluster to settle: how often it samples, how
  many identical samples in a row count as settled (one agreeing snapshot can
  be a transient state in the middle of a leader change), and how long a
  switchover, a candidate becoming ready or a healed network may take."
  {:poll-interval-ms 1000 :stable-samples 3 :timeout-ms 60000})

(defn- validate-convergence!
  "The `convergence` section (seconds in the YAML) as default-convergence's
  millisecond map."
  [path convergence]
  (when (some? convergence)
    (check-map! path "convergence" convergence))
  (let [poll-interval-seconds (get convergence :poll-interval-seconds 1)
        stable-samples (get convergence :stable-samples 3)
        timeout-seconds (get convergence :timeout-seconds 60)]
    (check-positive-int! path "convergence.poll-interval-seconds"
                         :poll-interval-seconds poll-interval-seconds)
    (check-positive-int! path "convergence.stable-samples" :stable-samples stable-samples)
    (check-positive-int! path "convergence.timeout-seconds" :timeout-seconds timeout-seconds)
    {:poll-interval-ms (* 1000 poll-interval-seconds)
     :stable-samples stable-samples
     :timeout-ms (* 1000 timeout-seconds)}))

(defn- validate-nemesis! [path nemesis]
  (when (some? nemesis)
    (check-map! path "nemesis" nemesis)
    (doseq [k [:cycles :duration-seconds :interval-seconds]]
      (check-positive-int! path (str "nemesis." (name k)) k (get nemesis k)))
    (select-keys nemesis [:cycles :duration-seconds :interval-seconds])))

(defn- validate-switchover! [path switchover]
  (when (some? switchover)
    (check-map! path "switchover" switchover))
  (let [max-attempts (or (:max-attempts switchover) 3)
        pause-seconds (get switchover :pause-seconds 60)]
    (check-positive-int! path "switchover.max-attempts" :max-attempts max-attempts)
    (when-not (and (integer? pause-seconds) (not (neg? pause-seconds)))
      (fail path "`switchover.pause-seconds` must be a non-negative integer"
            {:pause-seconds pause-seconds}))
    {:max-attempts max-attempts :pause-seconds pause-seconds}))

(defn- validate-pairs!
  "Check that FIELD of the profile is a list of pairs of configured node IDs."
  [path node-ids field pairs]
  (when (some? pairs)
    (when-not (sequential? pairs)
      (fail path (str "`" field "` must be a list of node ID pairs") {:value pairs}))
    (mapv (fn [pair]
            (when-not (and (sequential? pair) (= 2 (count pair)))
              (fail path (str "each `" field "` entry must be a pair of node IDs")
                    {:pair pair}))
            (doseq [node pair]
              (when-not (contains? (set node-ids) node)
                (fail path (str "`" field "` references unknown node `" node "`")
                      {:node node})))
            (vec pair))
          pairs)))

(defn- validate-transitions! [path node-ids transitions]
  (let [pairs (validate-pairs! path node-ids "transitions" transitions)]
    (when-let [[from] (first (filter (fn [[from to]] (= from to)) pairs))]
      (fail path "a transition must go between two different nodes" {:node from}))
    (when-not (= (count pairs) (count (set pairs)))
      (fail path "`transitions` contains duplicate pairs" {:transitions pairs}))
    pairs))

(declare plan-transitions)

(defn- check-plan!
  "Fail unless PROFILE's transitions can be walked as one Euler cycle, so a
  profile that cannot be run is rejected on load, not after the cluster is up."
  [path profile]
  (let [transitions (plan-transitions profile)]
    (when (empty? transitions)
      (fail path "the profile allows no switchover between nodes" {}))
    (when-not (plan/euler-tour transitions (:from (first transitions)))
      (fail path "the transitions do not form an Euler cycle (every node needs as many outgoing as incoming transitions, all connected)"
            {:transitions transitions}))
    profile))

(defn- validate-profile [path profile]
  (when-not (map? profile)
    (fail path "the YAML document must contain a mapping" {}))
  (when-not (and (sequential? (:nodes profile)) (<= 2 (count (:nodes profile))))
    (fail path "`nodes` must contain at least two nodes" {}))
  (when (contains? profile :topology)
    (fail path "`topology` is not supported: the expected topology is derived from `nodes`" {}))
  (let [nodes (mapv #(validate-node! path %) (:nodes profile))
        ids (mapv :id nodes)]
    (doseq [field [:id :ssh-host]]
      (when-not (apply distinct? (map field nodes))
        (fail path (str "node `" (name field) "` values must be unique")
              {:values (map field nodes)})))
    (let [transitions (validate-transitions! path ids (:transitions profile))
          connectivity (validate-pairs! path ids "connectivity" (:connectivity profile))]
      (->> (cond-> (assoc profile
                     :nodes nodes
                     :convergence (validate-convergence! path (:convergence profile))
                     :switchover (validate-switchover! path (:switchover profile))
                     :nemesis (validate-nemesis! path (:nemesis profile)))
        transitions (assoc :transitions transitions)
        connectivity (assoc :connectivity connectivity))
           (check-plan! path)))))

(defn load-profile
  "Load an HA profile from PATH; without one, from the file named by the
  JEPSEN_HA_PROFILE environment variable, else the default profile."
  ([] (load-profile (or (System/getenv "JEPSEN_HA_PROFILE")
                        default-profile-path)))
  ([path]
   (when-not (.isFile (io/file path))
     (fail path "file does not exist or is not a regular file" {}))
   (validate-profile path (try
                            (yaml/parse-string (slurp path))
                            (catch Throwable t
                              (throw (ex-info (str "Unable to read YAML profile " path ": "
                                                   (.getMessage t))
                                              {:profile path}
                                              t)))))))

(defn connected?
  "True when node B is reachable from node A: any two different nodes in a
  profile without `connectivity`, otherwise only the listed pairs (symmetric)."
  [profile a b]
  (and (not= a b)
       (if-let [pairs (:connectivity profile)]
         (boolean (some #(contains? #{[a b] [b a]} %) pairs))
         true)))

(defn- transition [from candidate]
  {:from from :candidate candidate})

(defn plan-transitions
  "Directed switchover transitions to attempt: the profile's `transitions`
  pairs, otherwise every ordered pair of connected nodes."
  [profile]
  (if-let [pairs (:transitions profile)]
    (mapv #(apply transition %) pairs)
    (let [ids (mapv :id (:nodes profile))]
      (vec (for [a ids, b ids :when (connected? profile a b)]
             (transition a b))))))

(defn expected-topology
  "Settled topology expected when LEADER is primary: everyone else is a
  replica, and every node is a member."
  [profile leader]
  (let [ids (mapv :id (:nodes profile))]
    {:leader leader
     :nodes (into {} (map (fn [id] [id {:postgres-role (if (= id leader) :primary :replica)}]))
                  ids)
     :members (set ids)}))
