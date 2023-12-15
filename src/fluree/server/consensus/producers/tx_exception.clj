(ns fluree.server.consensus.producers.tx-exception
  (:require [fluree.server.consensus.core :as consensus]
            [fluree.server.consensus.raft.core :as raft]))

(defn consensus-push-tx-exception
  [{:keys [:consensus/state-atom :consensus/raft-state :fluree/conn] :as config}
   {:keys [ledger-id tx-id] :as _params}
   tx-exception]
  (let [created-body {:ledger-id  ledger-id
                      :ex-message (ex-message tx-exception)
                      :ex-data    (ex-data tx-exception)
                      :tx-id      tx-id ;; for quickly removing from the queue
                      :server     (consensus/this-server raft-state)}]
    ;; returns promise
    (raft/leader-new-command! config :tx-exception created-body)))
