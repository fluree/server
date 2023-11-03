(ns fluree.server.components.consensus
  (:require [donut.system :as ds]
            [fluree.server.components.consensus-handler :as consensus-handler]
            [fluree.server.consensus.core :as consensus]
            [fluree.db.util.log :as log]))

(set! *warn-on-reflection* true)

(defn start-raft
  [config connection watcher subscriptions conn-storage-path]
  (log/debug "Starting raft consensus with config:" config)
  (let [handler (consensus-handler/create-handler consensus-handler/default-routes)]
    (consensus/start handler (assoc config :join? false
                                           :conn-storage-path conn-storage-path
                                           :fluree/conn connection
                                           :fluree/watcher watcher
                                           :fluree/subscriptions subscriptions))))

(def ^:const replicate-file-conn-types
  "These connection types will replicate the data on every fluree/server that is
  part of the network. Other connection types (e.g. S3, IPFS) should not be replicated
  as they are being stored by a central file service"
  #{:file :memory})

(def consensus
  #::ds{:start  (fn [{{:keys [config fluree/connection fluree/watcher fluree/subscriptions conn-storage-path]} ::ds/config}]
                  (let [{:keys [consensus-type]} config]
                    (case consensus-type
                      :raft (start-raft config connection watcher subscriptions conn-storage-path)
                      :none (log/info "No consensus, starting as query server."))))
        :stop   (fn [{::ds/keys [instance]}]
                  (when instance ((:close instance))))
        :config {:config               (ds/ref [:env :fluree/consensus])
                 :conn-storage-path    (ds/ref [:env :fluree/connection :storage-path])
                 :fluree/watcher       (ds/local-ref [:watcher])
                 :fluree/connection    (ds/local-ref [:conn])
                 :fluree/subscriptions (ds/ref [:fluree :subscriptions])}})
