(ns fluree.server.system
  (:require [fluree.db.connection.vocab :as conn-vocab]
            [fluree.server.config.vocab :as server-vocab]
            [fluree.db.util.core :as util :refer [get-id get-first get-first-value get-values]]
            [fluree.server.config :as config]
            [fluree.server.consensus.raft :as raft]
            [fluree.server.consensus.standalone :as standalone]
            [fluree.server.consensus.subscriptions :as subscriptions]
            [fluree.server.consensus.watcher :as watcher]
            [fluree.server.handler :as handler]
            [fluree.server.task.migrate-sid :as task.migrate-sid]
            [integrant.core :as ig]
            [ring.adapter.jetty9 :as jetty]))

(set! *warn-on-reflection* true)

(derive :fluree.server.consensus/raft :fluree.server/consensus)
(derive :fluree.server.consensus/standalone :fluree.server/consensus)

(derive :fluree.server.http/jetty :fluree.server/http)

(defn reference?
  [node]
  (and (map? node)
       (contains? node :id)
       (-> node (dissoc :idx :id) empty?)))

(defn convert-reference
  [node]
  (if (reference? node)
    (let [id (get-id node)]
      (ig/ref id))
    node))

(defn convert-node-references
  [node]
  (reduce-kv (fn [m k v]
               (let [v* (if (coll? v)
                          (mapv convert-reference v)
                          (convert-reference v))]
                 (assoc m k v*)))
             {} node))

(defn convert-references
  [cfg]
  (reduce-kv (fn [m id node]
               (assoc m id (convert-node-references node)))
             {} cfg))

(defmethod ig/expand-key :fluree.server/http
  [k config]
  (let [max-txn-wait-ms (get-first-value config server-vocab/max-txn-wait-ms)
        closed-mode     (get-first-value config server-vocab/closed-mode)
        root-identities (set (get-values config server-vocab/root-identities))
        config*         (-> config
                            (assoc :handler (ig/ref :fluree.server.api/handler))
                            (dissoc server-vocab/max-txn-wait-ms server-vocab/closed-mode
                                    server-vocab/root-identities))]
    {k                          config*
     :fluree.server/watcher     {:max-txn-wait-ms max-txn-wait-ms}
     :fluree.server.api/handler {:root-identities root-identities
                                 :closed-mode     closed-mode
                                 :connection      (ig/ref :fluree.db/connection)
                                 :consensus       (ig/ref :fluree.server/consensus)
                                 :watcher         (ig/ref :fluree.server/watcher)
                                 :subscriptions   (ig/ref :fluree.server/subscriptions)}}))

(defmethod ig/expand-key :fluree.server.consensus/standalone
  [k config]
  {k (assoc config
            :subscriptions (ig/ref :fluree.server/subscriptions)
            :watcher (ig/ref :fluree.server/watcher))})

(defmethod ig/init-key :fluree.server/subscriptions
  [_ _]
  (subscriptions/listen))

(defmethod ig/halt-key! :fluree.server/subscriptions
  [_ subsc]
  (subscriptions/close subsc))

(defmethod ig/init-key :fluree.server/watcher
  [_ {:keys [max-txn-wait-ms]}]
  (watcher/watch max-txn-wait-ms))

(defmethod ig/halt-key! :fluree.server/watcher
  [_ watcher]
  (watcher/close watcher))

(defmethod ig/init-key :fluree.server.consensus/raft
  [_ config]
  (let [log-history      (get-first-value config server-vocab/log-history)
        entries-max      (get-first-value config server-vocab/entries-max)
        catch-up-rounds  (get-first-value config server-vocab/catch-up-rounds)
        servers          (get-values config server-vocab/raft-servers)
        this-server      (get-first-value config server-vocab/this-server)
        log-directory    (get-first-value config server-vocab/log-directory)
        ledger-directory (get-first-value config server-vocab/ledger-directory)]
    (raft/start {:log-history      log-history
                 :entries-max      entries-max
                 :catch-up-rounds  catch-up-rounds
                 :servers          servers
                 :this-server      this-server
                 :log-directory    log-directory
                 :ledger-directory ledger-directory})))

(defmethod ig/halt-key! :fluree.server.consensus/raft
  [_ {:keys [close] :as _raft-group}]
  (close))

(defmethod ig/init-key :fluree.server.consensus/standalone
  [_ {:keys [subscriptions watcher] :as config}]
  (let [max-pending-txns (get-first-value config server-vocab/max-pending-txns)
        conn             (get-first config conn-vocab/connection)]
    (standalone/start conn subscriptions watcher max-pending-txns)))

(defmethod ig/halt-key! :fluree.server.consensus/standalone
  [_ transactor]
  (standalone/stop transactor))

(defmethod ig/init-key :fluree.server.api/handler
  [_ {:keys [connection consensus watcher subscriptions root-identities closed-mode]}]
  (handler/app connection consensus watcher subscriptions root-identities closed-mode))

(defmethod ig/init-key :fluree.server.http/jetty
  [_ {:keys [handler] :as config}]
  (let [port (get-first-value config server-vocab/http-port)]
    (jetty/run-jetty handler {:port port, :join? false})))

(defmethod ig/halt-key! :fluree.server.http/jetty
  [_ http-server]
  (jetty/stop-server http-server))

(defmethod ig/init-key :fluree.server/sid-migration
  [_ {:keys [conn ledgers force]}]
  (task.migrate-sid/migrate conn ledgers force))

(defmethod ig/init-key :default
  [_ config]
  config)

(def default-resource-name "file-config.jsonld")

(defn start-config
  ([config]
   (start-config config nil))
  ([config _profile]
   (-> config config/parse convert-references ig/expand ig/init)))

(defn start-file
  ([path]
   (start-file path :prod))
  ([path profile]
   (-> path
       config/load-file
       (start-config profile))))

(defn start-resource
  ([resource-name]
   (start-resource resource-name :prod))
  ([resource-name profile]
   (-> resource-name
       config/load-resource
       (start-config profile))))

(def start
  (partial start-resource default-resource-name))

(defn stop
  [server]
  (ig/halt! server))
