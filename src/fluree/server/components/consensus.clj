(ns fluree.server.components.consensus
  (:require [donut.system :as ds]
            [fluree.server.components.consensus-handler :as consensus-handler]
            [fluree.server.consensus.core :as consensus]
            [fluree.db.util.log :as log]))

(set! *warn-on-reflection* true)

(def consensus
  #::ds{:start  (fn [{{:keys [config handler conn conn-storage-path]} ::ds/config}]
                  (log/debug "Starting consensus with config:" config)
                  (consensus/start handler (assoc config :join? false
                                                         :ledger-directory conn-storage-path
                                                         :consensus-type :raft
                                                         :fluree/conn conn)))
        :stop   (fn [{::ds/keys [instance]}]
                  (when instance ((:close instance))))
        :config {:config  (ds/ref [:env :fluree/consensus])
                 :conn-storage-path (ds/ref [:env :fluree/consensus :storage-path])
                 :handler (ds/local-ref [:handler])
                 :conn    (ds/ref [:fluree :conn])}})


(def timeout-ms 2000)
(def heartbeat-ms (quot timeout-ms 3))


(def default-config
  {:server-configs     [{:server-id "SRV1" :host "localhost" :port 62071}
                        ;{:server-id "SRVB" :host "localhost" :port 62072}
                        ;{:server-id "SRVC" :host "localhost" :port 62073}
                        ]
   :routes             consensus-handler/default-routes
   :this-server        "SRV1"
   ;; TODO - trace down both uses of private-key vs private-keys reconcile
   :private-key        nil
   :private-keys       nil
   :log-directory      nil #_"./data/log/"
   :ledger-directory   nil #_"./data/ledger/"
   :encryption-key     nil
   :storage-type       :file
   :open-api           true})
