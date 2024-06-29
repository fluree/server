(ns fluree.server.components.consensus
  (:require [donut.system :as ds]
            [fluree.db.util.log :as log]
            [fluree.server.consensus.none :as none]
            [fluree.server.consensus.raft :as raft]))

(set! *warn-on-reflection* true)

(defn update-config
  [config connection watcher subscriptions conn-storage-path]
  (assoc config
         :ledger-directory conn-storage-path
         :fluree/conn connection
         :fluree/watcher watcher
         :fluree/subscriptions subscriptions))

(defn start-raft
  [config connection watcher subscriptions conn-storage-path]
  (log/debug "Starting raft consensus with config:" config)
  (-> (update-config config connection watcher subscriptions conn-storage-path)
      (assoc :join? false)
      (raft/start)))

(defn no-consensus
  [config connection watcher subscriptions conn-storage-path]
  (-> (update-config config connection watcher subscriptions conn-storage-path)
      (none/start)))

(def consensus
  #::ds{:start  (fn [{{:keys [config fluree/connection fluree/watcher fluree/subscriptions conn-storage-path]} ::ds/config}]
                  (let [{:keys [consensus-type]} config]
                    (case consensus-type
                      :raft (start-raft config connection watcher subscriptions conn-storage-path)
                      :none (no-consensus config connection watcher subscriptions conn-storage-path))))
        :stop   (fn [{::ds/keys [instance]}]
                  (when instance ((:close instance))))
        :config {:config               (ds/ref [:env :fluree/consensus])
                 :conn-storage-path    (ds/ref [:env :fluree/connection :storage-path])
                 :fluree/watcher       (ds/local-ref [:watcher])
                 :fluree/connection    (ds/local-ref [:conn])
                 :fluree/subscriptions (ds/ref [:fluree :subscriptions])}})
