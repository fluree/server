(ns user
  (:require [clojure.string :as str]
            [fluree.server.main :as main]
            [donut.system :as ds]
            [donut.system.repl :as donut]
            [donut.system.repl.state]
            [fluree.server.handlers.transact :as tx-handler]
            [fluree.server.handlers.create :as create-handler]
            [fluree.server.components.consensus :as consensus]
            [fluree.server.components.consensus-handler :as consensus-handler]
            [fluree.server.consensus.raft.core]
            [clojure.java.io :as io]
            [fluree.server.main :as server]
            [fluree.db.util.log :as log]))

(def system nil)

;; overwrite http-api's REPL config
(defmethod ds/named-system ::ds/repl
  [_]
  (ds/system :dev))


;; Three Server Configuration
(def server-1 "/ip4/127.0.0.1/tcp/62071")
(def server-2 "/ip4/127.0.0.1/tcp/62072")
(def server-3 "/ip4/127.0.0.1/tcp/62073")
(def servers-str (str/join "," [server-1 server-2 server-3]))

(def server-1-env (-> (server/env-config :dev)
                      (assoc-in [:http/server :port] 58090)
                      (assoc-in [:fluree/connection :storage-path] "data/srv1")
                      (assoc-in [:fluree/consensus :consensus-servers] servers-str)
                      (assoc-in [:fluree/consensus :consensus-this-server] server-1)))
(def server-2-env (-> (server/env-config :dev)
                      (assoc-in [:http/server :port] 58091)
                      (assoc-in [:fluree/connection :storage-path] "data/srv2")
                      (assoc-in [:fluree/consensus :consensus-servers] servers-str)
                      (assoc-in [:fluree/consensus :consensus-this-server] server-2)))
(def server-3-env (-> (server/env-config :dev)
                      (assoc-in [:http/server :port] 58092)
                      (assoc-in [:fluree/connection :storage-path] "data/srv3")
                      (assoc-in [:fluree/consensus :consensus-servers] servers-str)
                      (assoc-in [:fluree/consensus :consensus-this-server] server-3)))


(def query-server-1-env (-> (server/env-config :dev)
                            (assoc-in [:http/server :port] 58095)
                            (assoc-in [:fluree/connection :servers] "http://localhost:58090")
                            (assoc-in [:fluree/connection :method] :remote)
                            (assoc-in [:fluree/consensus :consensus-type] :none)))

(defmethod ds/named-system ::srv1
  [_]
  (ds/system :dev {[:env] server-1-env}))

(defmethod ds/named-system ::srv2
  [_]
  (ds/system :dev {[:env] server-2-env}))

(defmethod ds/named-system ::srv3
  [_]
  (ds/system :dev {[:env] server-3-env}))


(defmethod ds/named-system ::query1
  [_]
  (ds/system :dev {[:env] query-server-1-env}))


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

(defn start-server
  "Supply"
  [config-name]
  (let [sys (donut.system/start config-name)]
    (alter-var-root #'system (constantly sys))
    ::server-started!))

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

  (start-server ::query1)


  ;; starting a 3 server cluster
  (start-server ::srv1)
  (start-server ::srv2)
  (start-server ::srv3))
