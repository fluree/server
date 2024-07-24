(ns fluree.server.system
  (:require [fluree.db.api :as fluree]
            [fluree.server.config :as config]
            [fluree.server.consensus.raft :as raft]
            [fluree.server.consensus.standalone :as standalone]
            [fluree.server.consensus.subscriptions :as subscriptions]
            [fluree.server.consensus.watcher :as watcher]
            [fluree.server.handler :as handler]
            [integrant.core :as ig]
            [ring.adapter.jetty9 :as jetty]))

(set! *warn-on-reflection* true)

(def default-resource-name "config.json")

(derive :fluree/raft :fluree/consensus)
(derive :fluree/standalone :fluree/consensus)
(derive :http/jetty :http/server)

(defmethod ig/expand-key ::config/connection
  [_ config]
  {:fluree/connection config})

(defmethod ig/expand-key ::config/consensus
  [_ config]
  (let [consensus-key    (keyword "fluree" (-> config :protocol name))
        consensus-config (-> config
                             (dissoc :protocol)
                             (assoc :conn (ig/ref :fluree/connection)
                                    :watcher (ig/ref :fluree/watcher)
                                    :subscriptions (ig/ref :fluree/subscriptions)))]
    {consensus-key         consensus-config
     :fluree/subscriptions {}}))

(defmethod ig/expand-key ::config/http
  [_ {:keys [server max-txn-wait-ms] :as config :or {server :jetty}}]
  (let [http-key    (keyword "http" (name server))
        http-config (-> config
                        (dissoc :server :max-txn-wait-ms)
                        (assoc :handler (ig/ref :fluree/handler)))]
    {http-key        http-config
     :fluree/watcher {:max-txn-wait-ms max-txn-wait-ms}
     :fluree/handler {:conn          (ig/ref :fluree/connection)
                      :consensus     (ig/ref :fluree/consensus)
                      :watcher       (ig/ref :fluree/watcher)
                      :subscriptions (ig/ref :fluree/subscriptions)}}))

(defmethod ig/init-key :fluree/connection
  [_ {:keys [storage-method] :as config}]
  (let [config* (-> config
                    (assoc :method storage-method)
                    (dissoc :storage-method))]
    @(fluree/connect config*)))

(defmethod ig/init-key :fluree/subscriptions
  [_ _]
  (subscriptions/listen))

(defmethod ig/halt-key! :fluree/subscriptions
  [_ subs]
  (subscriptions/close subs))

(defmethod ig/init-key :fluree/watcher
  [_ {:keys [max-txn-wait-ms]}]
  (watcher/watch max-txn-wait-ms))

(defmethod ig/halt-key! :fluree/watcher
  [_ watcher]
  (watcher/close watcher))

(defmethod ig/init-key :fluree/raft
  [_ config]
  (raft/start config))

(defmethod ig/halt-key! :fluree/raft
  [_ {:keys [close] :as _raft-group}]
  (close))

(defmethod ig/init-key :fluree/standalone
  [_ {:keys [conn subscriptions watcher max-pending-txns]}]
  (standalone/start conn subscriptions watcher max-pending-txns))

(defmethod ig/halt-key! :fluree/standalone
  [_ transactor]
  (standalone/stop transactor))

(defmethod ig/init-key :fluree/handler
  [_ {:keys [conn consensus watcher subscriptions]}]
  (handler/app conn consensus watcher subscriptions))

(defmethod ig/init-key :http/jetty
  [_ {:keys [handler port join?]}]
  (jetty/run-jetty handler {:port port, :join? join?}))

(defmethod ig/halt-key! :http/jetty
  [_ http-server]
  (jetty/stop-server http-server))

(defn start-config
  [config]
  (-> config ig/expand ig/init))

(defn start-file
  ([path]
   (start-file path :prod))
  ([path profile]
   (-> path
       (config/load-file profile)
       start-config)))

(defn start-resource
  ([resource-name]
   (start-resource resource-name :prod))
  ([resource-name profile]
   (-> resource-name
       (config/load-resource profile)
       start-config)))

(def start
  (partial start-resource default-resource-name))

(defn stop
  [server]
  (ig/halt! server))
