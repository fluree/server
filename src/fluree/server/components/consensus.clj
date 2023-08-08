(ns fluree.server.components.consensus
  (:require [donut.system :as ds]
            [fluree.server.components.consensus-handler :as consensus-handler]
            [fluree.server.consensus.core :as consensus]
            [fluree.db.util.log :as log]))

(set! *warn-on-reflection* true)

(def consensus
  #::ds{:start  (fn [{{:keys [config fluree/connection fluree/watcher conn-storage-path]} ::ds/config}]
                  (log/debug "Starting consensus with config:" config)
                  (let [handler (consensus-handler/create-handler consensus-handler/default-routes)]
                    (consensus/start handler (assoc config :join? false
                                                           :ledger-directory conn-storage-path
                                                           :consensus-type :raft
                                                           :fluree/conn connection
                                                           :fluree/watcher watcher))))
        :stop   (fn [{::ds/keys [instance]}]
                  (when instance ((:close instance))))
        :config {:config            (ds/ref [:env :fluree/consensus])
                 :conn-storage-path (ds/ref [:env :fluree/connection :storage-path])
                 :fluree/watcher    (ds/local-ref [:watcher])
                 :fluree/connection (ds/local-ref [:conn])}})
