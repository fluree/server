(ns fluree.server.system
  (:require [clojure.string :as str]
            [fluree.db :as-alias db]
            [fluree.db.connection.config :as conn-config]
            [fluree.db.connection.system :as conn-system]
            [fluree.db.connection.vocab :as conn-vocab]
            [fluree.db.util :as util :refer [get-first of-type?]]
            [fluree.db.util.json :as json]
            [fluree.db.util.log :as log]
            [fluree.server :as-alias server]
            [fluree.server.broadcast.subscriptions :as subscriptions]
            [fluree.server.config :as config]
            [fluree.server.config.vocab :as server-vocab]
            [fluree.server.consensus :as-alias consensus]
            [fluree.server.consensus.raft :as raft]
            [fluree.server.consensus.standalone :as standalone]
            [fluree.server.handler :as handler]
            [fluree.server.http :as-alias http]
            [fluree.server.watcher :as watcher]
            [integrant.core :as ig]
            [ring.adapter.jetty9 :as jetty]))

(set! *warn-on-reflection* true)

(derive ::consensus/raft ::server/consensus)
(derive ::consensus/standalone ::server/consensus)
(derive ::server/subscriptions ::server/broadcast)
(derive ::http/jetty ::server/http)

(defn parse-cors-origins
  "Parse CORS origins from config. Supports strings, regex patterns, and special values."
  [origins]
  (when origins
    (mapv (fn [origin]
            (cond
              ;; Special case for wildcard
              (= origin "*") #".*"
              ;; Regex pattern (starts with ^)
              (and (string? origin) (str/starts-with? origin "^"))
              (re-pattern origin)
              ;; Plain string origin
              :else origin))
          origins)))

(defmethod ig/expand-key ::server/http
  [k config]
  (let [max-txn-wait-ms (conn-config/get-first-integer config server-vocab/max-txn-wait-ms)
        closed-mode     (conn-config/get-first-boolean config server-vocab/closed-mode)
        root-identities (set (conn-config/get-strings config server-vocab/root-identities))
        cors-origins    (parse-cors-origins (conn-config/get-strings config server-vocab/cors-origins))
        config*         (-> config
                            (assoc :handler (ig/ref ::server/handler))
                            (dissoc server-vocab/max-txn-wait-ms server-vocab/closed-mode
                                    server-vocab/root-identities server-vocab/cors-origins))]
    {k                config*
     ::server/watcher {:max-txn-wait-ms max-txn-wait-ms}
     ::server/handler {:root-identities root-identities
                       :closed-mode     closed-mode
                       :cors-origins    cors-origins
                       :connection      (ig/ref ::db/connection)
                       :consensus       (ig/ref ::server/consensus)
                       :watcher         (ig/ref ::server/watcher)
                       :subscriptions   (ig/ref ::server/subscriptions)}}))

(defmethod ig/expand-key ::consensus/standalone
  [k config]
  {k (assoc config
            :watcher (ig/ref ::server/watcher)
            :broadcaster (ig/ref ::server/broadcast))})

(defmethod ig/expand-key ::consensus/raft
  [k config]
  {k (assoc config
            :watcher (ig/ref ::server/watcher)
            :broadcaster (ig/ref ::server/broadcast))})

(defmethod ig/init-key ::server/subscriptions
  [_ _]
  (subscriptions/listen))

(defmethod ig/halt-key! ::server/subscriptions
  [_ subs]
  (subscriptions/close subs))

(defmethod ig/init-key ::server/watcher
  [_ {:keys [max-txn-wait-ms]}]
  (if max-txn-wait-ms
    (watcher/start max-txn-wait-ms)
    (watcher/start)))

(defmethod ig/halt-key! ::server/watcher
  [_ watcher]
  (watcher/stop watcher))

(defmethod ig/init-key ::consensus/raft
  [_ {:keys [watcher broadcaster] :as config}]
  (let [log-history      (conn-config/get-first-integer config server-vocab/log-history)
        entries-max      (conn-config/get-first-integer config server-vocab/entries-max)
        catch-up-rounds  (conn-config/get-first-integer config server-vocab/catch-up-rounds)
        servers          (conn-config/get-strings config server-vocab/raft-servers)
        this-server      (conn-config/get-first-string config server-vocab/this-server)
        log-directory    (conn-config/get-first-string config server-vocab/log-directory)
        ledger-directory (conn-config/get-first-string config server-vocab/ledger-directory)]
    (log/info "Starting Raft consensus mode")
    (log/info "  This server:" this-server)
    (log/info "  Cluster servers:" (str/join ", " servers))
    (log/info "  Log directory:" log-directory)
    (log/info "  Ledger directory:" ledger-directory)
    (raft/start {:log-history        log-history
                 :entries-max        entries-max
                 :catch-up-rounds    catch-up-rounds
                 :servers            servers
                 :this-server        this-server
                 :log-directory      log-directory
                 :ledger-directory   ledger-directory
                 :fluree/watcher     watcher
                 :fluree/broadcaster broadcaster})))

(defmethod ig/halt-key! ::consensus/raft
  [_ {:keys [close] :as _raft-group}]
  (close))

(defmethod ig/init-key ::consensus/standalone
  [_ {:keys [watcher broadcaster] :as config}]
  (let [max-pending-txns (conn-config/get-first-integer config server-vocab/max-pending-txns)
        conn             (get-first config conn-vocab/connection)]
    (standalone/start conn watcher broadcaster max-pending-txns)))

(defmethod ig/halt-key! ::consensus/standalone
  [_ transactor]
  (standalone/stop transactor))

(defmethod ig/init-key ::server/handler
  [_ config]
  (-> config
      (select-keys [:connection :consensus :watcher :subscriptions :root-identities :closed-mode])
      handler/app))

(defmethod ig/init-key ::http/jetty
  [_ {:keys [handler] :as config}]
  (let [port (conn-config/get-first-integer config server-vocab/http-port)]
    (jetty/run-jetty handler {:port port, :join? false})))

(defmethod ig/halt-key! ::http/jetty
  [_ http-server]
  (jetty/stop-server http-server))

(defmethod ig/init-key :default
  [_ config]
  config)

(def default-resource-name "file-config.jsonld")

(defn log-config-summary
  [parsed-config]
  (let [connection-config (some #(when (of-type? % conn-vocab/connection-type) %)
                                (vals parsed-config))
        storage-config    (some #(when (of-type? % conn-vocab/storage-type) %)
                                (vals parsed-config))
        consensus-config  (some #(when (config/consensus? %) %)
                                (vals parsed-config))
        http-config       (some #(when (config/http-api? %) %)
                                (vals parsed-config))
        cache-mb          (when connection-config
                            (conn-config/get-first-integer connection-config conn-vocab/cache-max-mb))
        storage-type      (when storage-config
                            (let [storage-id (:id storage-config)]
                              (cond
                                (conn-config/get-first-string storage-config conn-vocab/file-path)
                                (str "File storage at " (conn-config/get-first-string storage-config conn-vocab/file-path))

                                (conn-config/get-first-string storage-config conn-vocab/s3-bucket)
                                (str "S3 storage (bucket: " (conn-config/get-first-string storage-config conn-vocab/s3-bucket) ")")

                                (conn-config/get-first-string storage-config conn-vocab/ipfs-endpoint)
                                (str "IPFS storage (endpoint: " (conn-config/get-first-string storage-config conn-vocab/ipfs-endpoint) ")")

                                :else
                                (str "Storage type: " (name (or storage-id "unknown"))))))
        consensus-type   (when consensus-config
                           (util/get-first-value consensus-config server-vocab/consensus-protocol))
        max-pending-txns (when (and consensus-config (= "standalone" consensus-type))
                           (conn-config/get-first-integer consensus-config server-vocab/max-pending-txns))
        http-port        (when http-config
                           (conn-config/get-first-integer http-config server-vocab/http-port))
        closed-mode      (when http-config
                           (conn-config/get-first-boolean http-config server-vocab/closed-mode))]
    (log/info "Server configuration summary:")
    (log/info "  HTTP port:" (or http-port "Not configured"))
    (log/info "  Consensus mode:" (or consensus-type "Not configured"))
    (when max-pending-txns
      (log/info "  Max pending transactions:" max-pending-txns))
    (log/info "  Storage:" (or storage-type "In-memory"))
    (log/info "  Cache size:" (if cache-mb
                                (str cache-mb " MB")
                                "Not configured"))
    (when closed-mode
      (log/info "  Closed mode: enabled"))))

(defn start-config
  [config & {:keys [profile reindex]}]
  (let [json-config (if (string? config)
                      (json/parse config false)
                      config)
        config-with-profile (if profile
                              (config/apply-profile json-config profile)
                              json-config)
        parsed-config (config/parse config-with-profile)]
    (log-config-summary parsed-config)
    (conn-system/initialize parsed-config)))

(defn start-file
  [path & {:keys [profile reindex]}]
  (log/info "Loading configuration from file:" path)
  (-> path
      config/read-file
      (start-config :profile profile :reindex reindex)))

(defn start-resource
  [resource-name & {:keys [profile reindex]}]
  (log/info "Loading configuration from resource:" resource-name)
  (-> resource-name
      config/read-resource
      (start-config :profile profile :reindex reindex)))

(defn start
  [& {:keys [profile reindex]}]
  (start-resource default-resource-name :profile profile :reindex reindex))

(defn stop
  [server]
  (ig/halt! server))
