(ns fluree.server.system
  (:require [fluree.db :as-alias db]
            [fluree.db.connection.config :as conn-config]
            [fluree.db.connection.system :as conn-system]
            [fluree.db.connection.vocab :as conn-vocab]
            [fluree.db.util.core :as util :refer [get-first]]
            [fluree.server :as-alias server]
            [fluree.server.broadcast.subscriptions :as subscriptions]
            [fluree.server.config :as config]
            [fluree.server.config.vocab :as server-vocab]
            [fluree.server.consensus :as-alias consensus]
            [fluree.server.consensus.raft :as raft]
            [fluree.server.consensus.standalone :as standalone]
            [fluree.server.handler :as handler]
            [fluree.server.http :as-alias http]
            [fluree.server.task.migrate-sid :as task.migrate-sid]
            [fluree.server.watcher :as watcher]
            [integrant.core :as ig]
            [ring.adapter.jetty9 :as jetty]))

(set! *warn-on-reflection* true)

(derive ::consensus/raft ::server/consensus)
(derive ::consensus/standalone ::server/consensus)
(derive ::server/subscriptions ::server/broadcast)
(derive ::http/jetty ::server/http)

(defmethod ig/expand-key ::server/http
  [k config]
  (let [max-txn-wait-ms (conn-config/get-first-integer config server-vocab/max-txn-wait-ms)
        closed-mode     (conn-config/get-first-boolean config server-vocab/closed-mode)
        root-identities (set (conn-config/get-strings config server-vocab/root-identities))
        config*         (-> config
                            (assoc :handler (ig/ref ::server/handler))
                            (dissoc server-vocab/max-txn-wait-ms server-vocab/closed-mode
                                    server-vocab/root-identities))]
    {k                config*
     ::server/watcher {:max-txn-wait-ms max-txn-wait-ms}
     ::server/handler {:root-identities root-identities
                       :closed-mode     closed-mode
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

(defmethod ig/init-key ::server/sid-migration
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
   (-> config config/parse conn-system/initialize)))

(defn start-file
  ([path]
   (start-file path :prod))
  ([path profile]
   (-> path
       config/read-file
       (start-config profile))))

(defn start-resource
  ([resource-name]
   (start-resource resource-name :prod))
  ([resource-name profile]
   (-> resource-name
       config/read-resource
       (start-config profile))))

(def start
  (partial start-resource default-resource-name))

(defn stop
  [server]
  (ig/halt! server))
