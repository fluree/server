(ns user
  (:require [fluree.server.main :as main]
            [donut.system :as ds]
            [donut.system.repl :as donut]
            [fluree.server.handlers.transact :as tx-handler]
            [fluree.server.handlers.create :as create-handler]
            [fluree.server.components.consensus :as consensus]
            [fluree.server.components.consensus-handler :as consensus-handler]
            [clojure.java.io :as io]
            [fluree.db.util.log :as log]))

(def system nil)

(def default-config
  {:http/server       {:port           8091
                       :middleware     [[0 main/wrap-log-request]
                                        [0 main/wrap-log-response]
                                        [90 main/wrap-record-fuel-usage]]
                       :tx-handler     tx-handler/default
                       :create-handler create-handler/default
                       },
   :fluree/connection {:method       :file,
                       :parallelism  2,
                       :storage-path "data/ledger",
                       :defaults     {:context {:id   "@id",
                                                :type "@type",
                                                :ex   "http://example.com/"}
                                      :indexer {:reindex-min-bytes 9
                                                :reindex-max-bytes 10000000}}}
   :fluree/consensus  consensus/default-config})


;; overwrite http-api's REPL config
(defmethod ds/named-system ::ds/repl
  [_]
  (ds/system :base {[:env]                                         default-config
                    [:consensus]                                   {:group   consensus/consensus
                                                                    :handler consensus-handler/start}
                    [:http :handler ::ds/config :fluree/consensus] (ds/ref [:consensus :group])}))


(def raft-server-configs [{:server-id "srv1" :host "localhost" :port 62071}
                          {:server-id "srv2" :host "localhost" :port 62072}
                          {:server-id "srv3" :host "localhost" :port 62073}])

;; Three Server Configuration
(defmethod ds/named-system ::srv1
  [_]
  (ds/system :base {[:env]
                    (-> default-config
                        (assoc-in [:http/server :port] 8091)
                        (assoc-in [:fluree/consensus :server-configs] raft-server-configs)
                        (assoc-in [:fluree/consensus :this-server] "srv1")
                        (assoc-in [:fluree/connection :storage-path] "data/srv1/ledger"))

                    [:consensus]
                    {:group   consensus/consensus
                     :handler consensus-handler/start}

                    [:http :handler ::ds/config :fluree/consensus]
                    (ds/ref [:consensus :group])}))


(defmethod ds/named-system ::srv2
  [_]
  (ds/system :base {[:env]
                    (-> default-config
                        (assoc-in [:http/server :port] 8092)
                        (assoc-in [:fluree/consensus :server-configs] raft-server-configs)
                        (assoc-in [:fluree/consensus :this-server] "srv2")
                        (assoc-in [:fluree/connection :storage-path] "data/srv2/ledger"))

                    [:consensus]
                    {:group   consensus/consensus
                     :handler consensus-handler/start}

                    [:http :handler ::ds/config :fluree/consensus]
                    (ds/ref [:consensus :group])}))

(defmethod ds/named-system ::srv3
  [_]
  (ds/system :base {[:env]
                    (-> default-config
                        (assoc-in [:http/server :port] 8093)
                        (assoc-in [:fluree/consensus :server-configs] raft-server-configs)
                        (assoc-in [:fluree/consensus :this-server] "srv3")
                        (assoc-in [:fluree/connection :storage-path] "data/srv3/ledger"))

                    [:consensus]
                    {:group   consensus/consensus
                     :handler consensus-handler/start}

                    [:http :handler ::ds/config :fluree/consensus]
                    (ds/ref [:consensus :group])}))


(defn donut-system
  "Returns the entire 'system' created by donut.
  If using ::ds/repl, donut maintains its own var holding it, for other
  custom profiles we store it in a global var."
  []
  ;; in raft mode, system stored in #'system, else donut stores in REPL
  (or system donut.system.repl.state/system))

(defn view-raft-state-machine
  "View raft state machine state of running system."
  []
  (-> (donut-system)
      ::ds/instances
      :consensus
      :group
      :state-atom
      deref))

(defn view-raft-state
  "View internal raft state of running system."
  []
  (let [p        (promise)
        raft-map (-> (donut-system)
                     ::ds/instances
                     :consensus
                     :group)
        callback (fn [state]
                   (deliver p (dissoc state :config)))]
    (fluree.server.consensus.raft.core/get-raft-state raft-map callback)
    p))

(defn start-raft-server
  "Supply"
  [config-name]
  (let [sys (donut.system/start config-name)]
    (alter-var-root #'system (constantly sys))))

(comment

  (donut/start)

  (donut/restart)

  ;; view the state-machine data that raft maintains for us
  (view-raft-state-machine)
  ;; view raft's internal state
  @(view-raft-state)
  ;; read a raft log file
  (fluree.raft.log/read-log-file (io/file "data/srv1/raftlog/0.raft"))
  ;;


  ;; starting a 3 server cluster
  (start-raft-server ::srv1)
  (start-raft-server ::srv2)
  (start-raft-server ::srv3)

  )



