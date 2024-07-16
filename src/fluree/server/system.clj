(ns fluree.server.system
  (:require [aero.core :as aero]
            [clojure.java.io :as io]
            [fluree.db.api :as fluree]
            [fluree.server.consensus.raft :as raft]
            [fluree.server.subscriptions :as subscriptions]
            [fluree.server.handler :as handler]
            [fluree.server.watcher :as watcher]
            [integrant.core :as ig]
            [meta-merge.core :refer [meta-merge]]
            [ring.adapter.jetty9 :as jetty]))

(def default-resource-name "config.edn")

(defmethod aero/reader 'ig/ref
  [_ _ ref-key]
  (ig/ref ref-key))

(derive :fluree/raft :fluree/consensus)
(derive :fluree/standalone :fluree/consensus)
(derive :http/jetty :http/server)

(defmethod ig/init-key :fluree/connection
  [_ config]
  @(fluree/connect config))

(defmethod ig/init-key :fluree/subscriptions
  [_ _]
  (subscriptions/listen))

(defmethod ig/halt-key! :fluree/subscriptions
  [_ subs]
  (subscriptions/close subs))

(defmethod ig/init-key :fluree/watcher
  [_ {:keys [max-tx-wait-ms]}]
  (watcher/watch max-tx-wait-ms))

(defmethod ig/halt-key! :fluree/watcher
  [_ watcher]
  (watcher/close watcher))

(defmethod ig/init-key :fluree/raft
  [_ config]
  (raft/start config))

(defmethod ig/halt-key! :fluree/raft
  [_ {:keys [close] :as _raft-group}]
  (close))

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
  ([config]
   (start-config config {}))
  ([config overrides]
   (-> config
       (meta-merge overrides)
       ig/init)))

(defn start-resource
  ([resource-name]
   (start-resource resource-name :prod))
  ([resource-name profile]
   (start-resource resource-name profile {}))
  ([resource-name profile overrides]
   (-> resource-name
       io/resource
       (aero/read-config {:profile profile})
       (start-config overrides))))

(def start
  (partial start-resource default-resource-name))

(defn stop
  [server]
  (ig/halt! server))
