(ns jepsen.ha.patroni-system
  "Patroni cluster access: observation through the REST API, candidate
  readiness, and the switchover through patronictl over SSH."
  (:require [clojure.data.json :as json]
            [clojure.set :as set]
            [clojure.string :as string]
            [clojure.tools.logging :refer [warn]]
            [jepsen.control :as control]
            [org.httpkit.client :as http]))

(def ^:private request-timeout-ms
  "How long one REST API request may take."
  3000)

(def ^:private body-limit
  "How many characters of a health check's response body are kept."
  4096)

(defn- http-get
  "GET path from a Patroni REST API with basic auth. Returns {:status :body}
  for any HTTP status; throws on connection failure."
  [{:keys [restapi-username restapi-password]} rest-api path]
  (let [{:keys [status body error]}
        @(http/get (str rest-api path)
                   {:basic-auth [restapi-username restapi-password] :as :text :timeout request-timeout-ms})]
    (when error (throw error))
    {:status status :body body}))

(defn- get-json [system rest-api path]
  (let [{:keys [status body]} (http-get system rest-api path)]
    (when-not (= 200 status)
      (throw (ex-info (str "Patroni " path " returned HTTP " status) {:status status})))
    (json/read-str body :key-fn keyword)))

(defn candidate-readiness
  "Normalize Patroni /sync and /quorum health checks ({path {:status n ...}} or
  {path {:error msg}}) for one candidate."
  [checks]
  (let [status #(get-in checks [% :status])]
    (cond
      (= 200 (status "/sync"))
      {:ready? true :reason :sync-standby}

      (= 200 (status "/quorum"))
      {:ready? true :reason :quorum-standby}

      (every? :status (vals checks))
      {:ready? false :reason :candidate-not-sync-standby :checks checks}

      :else
      {:ready? false :reason :candidate-readiness-unavailable :checks checks})))

(defn- normalized-postgres-role [role]
  (case role
    "primary" :primary
    "replica" :replica
    :unknown))

(defn- normalized-ha-role [role]
  (case role
    "leader" :leader
    "standby_leader" :standby-leader
    "replica" :replica
    "sync_standby" :sync-standby
    "quorum_standby" :quorum-standby
    :unknown))

(defn- normalize-member [member member-name->id]
  (let [ha-role (normalized-ha-role (:role member))]
    {:node (get member-name->id (:name member) (:name member))
     :ha-role ha-role
     :postgres-role (case ha-role :leader :primary :unknown :unknown :replica)
     :state (some-> (:state member) keyword)}))

(defn discover-node-names!
  "Map node ids to Patroni member names by asking each node's own /patroni.
  The result is cached in the system."
  [{:keys [node-names] :as system} test]
  (locking node-names
    (or (not-empty @node-names)
        (let [names (into {}
                          (pmap (fn [[node {:keys [rest-api]}]]
                                  [node (get-in (get-json system rest-api "/patroni")
                                                [:patroni :name])])
                                (:ha-node-config test)))]
          (when-not (and (every? #(not (string/blank? %)) (vals names))
                         (apply distinct? (vals names)))
            (throw (ex-info "Patroni did not report a unique member name for every node"
                            {:names names})))
          (reset! node-names names)))))

(defn node-ids
  "The logical node ids of the cluster under test, sorted."
  [test]
  (keys (:ha-node-config test)))

(defn ready?
  "{:ready? bool :reason kw}: whether the transition's candidate may be
  switched to now."
  [system test {:keys [candidate]}]
  (let [rest-api (get-in test [:ha-node-config candidate :rest-api])]
    (candidate-readiness
      (into {}
            (for [path ["/sync" "/quorum"]]
              [path (try
                      (if rest-api
                        (update (http-get system rest-api path) :body
                                #(subs % 0 (min body-limit (count %))))
                        {:error (str "No REST API configured for " candidate)})
                      (catch Throwable t
                        {:error (or (.getMessage t) (str t))}))])))))

(defn switchover!
  "Issue the switchover. Returns {:result :switchover-issued ...} or
  {:error :switchover-command-failed :message str}; never throws."
  [{:keys [command-timeout-seconds] :as system} test {:keys [from candidate]}]
  (let [executor (first (remove #{candidate} (node-ids test)))
        executor-host (get-in test [:ha-node-config executor :ssh-host])
        context {:from from :candidate candidate
                 :executor executor :executor-host executor-host}]
    (try
      (let [names (discover-node-names! system test)]
        (when-not (and executor-host (names from) (names candidate))
          (throw (ex-info "Patroni switchover profile is missing a node mapping" context)))
        (control/on executor-host
          (control/exec :timeout (str command-timeout-seconds) :patronictl :switchover
                        :--leader (names from)
                        :--candidate (names candidate)
                        :--force))
        (assoc context :result :switchover-issued))
      (catch Throwable t
        (warn (str "Unable to run switch: "
                   (first (string/split-lines (or (.getMessage t) (str t))))))
        (assoc context :error :switchover-command-failed
                       :message (or (.getMessage t) (str t)))))))

(defn observe
  "{:node :reachable? :postgres-role :ha-role :leader :members ...} from the
  perspective of NODE; never throws."
  [system test node]
  (try
    (let [rest-api (get-in test [:ha-node-config node :rest-api])
          member-name->id (set/map-invert (discover-node-names! system test))
          status (get-json system rest-api "/patroni")
          cluster (get-json system rest-api "/cluster")
          members (mapv #(normalize-member % member-name->id) (:members cluster))
          local-id (get member-name->id (get-in status [:patroni :name]) node)]
      {:node node
       :reachable? true
       :postgres-role (normalized-postgres-role (:role status))
       :ha-role (:ha-role (some #(when (= local-id (:node %)) %) members))
       :state (some-> (:state status) keyword)
       :leader (some #(when (#{:leader :standby-leader} (:ha-role %)) (:node %))
                     members)
       :members members})
    (catch Throwable t
      {:node node
       :reachable? false
       :postgres-role :unknown
       :ha-role :unknown
       :error (or (.getMessage t) (str t))})))

(defn node-config
  "Per-node Patroni settings ({node-id {:rest-api url}}) from the profile's
  `patroni` section, checked against the profile's nodes."
  [profile]
  (let [fail (fn [message data]
               (throw (ex-info (str "Invalid `patroni` profile section: " message) data)))
        raw (get-in profile [:patroni :nodes])
        settings (into {} (map (fn [[id s]] [(name id) s])) raw)
        ids (mapv :id (:nodes profile))]
    (when-not (map? raw)
      (fail "`patroni.nodes` must be a mapping" {:value raw}))
    (when-not (= (set ids) (set (keys settings)))
      (fail "`patroni.nodes` must contain exactly the configured node IDs"
            {:node-ids (set ids) :patroni-node-ids (set (keys settings))}))
    (doseq [id ids
            :let [rest-api (get-in settings [id :rest-api])]]
      (when (string/blank? (when (string? rest-api) rest-api))
        (fail (str "`patroni.nodes." id ".rest-api` must be a non-empty string")
              {:node-id id :rest-api rest-api})))
    (let [rest-apis (map #(get-in settings [% :rest-api]) ids)]
      (when-not (apply distinct? rest-apis)
        (fail "REST API endpoints must be unique" {:rest-apis rest-apis})))
    (into {} (map (fn [id] [id (select-keys (settings id) [:rest-api])])) ids)))

(def default-command-timeout-seconds
  "How long patronictl may run before it is killed."
  10)

(defn patroni-system
  "Create a Patroni HA system with explicit REST API credentials and the
  patronictl time limit (:command-timeout-seconds, default 10)."
  [{:keys [restapi-username restapi-password command-timeout-seconds]
    :or {command-timeout-seconds default-command-timeout-seconds}}]
  (when-not (and (integer? command-timeout-seconds) (pos? command-timeout-seconds))
    (throw (ex-info "Invalid `patroni` profile section: `command-timeout-seconds` must be a positive integer"
                    {:command-timeout-seconds command-timeout-seconds})))
  (when (or (string/blank? restapi-username)
            (string/blank? restapi-password))
    (throw (ex-info "Patroni REST API credentials are required"
                    {:username-present? (boolean (seq restapi-username))
                     :password-present? (boolean (seq restapi-password))})))
  {:restapi-username restapi-username
   :restapi-password restapi-password
   :command-timeout-seconds command-timeout-seconds
   :node-names (atom {})})
