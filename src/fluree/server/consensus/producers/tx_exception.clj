(ns fluree.server.consensus.producers.tx-exception
  (:require [fluree.server.consensus.raft :as raft]))

(defn consensus-push-tx-exception
  [{:keys [consensus/raft-state] :as config}
   {:keys [ledger-id tx-id] :as _params}
   tx-exception]
  (let [created-body {:ledger-id  ledger-id
                      :ex-message (ex-message tx-exception)
                      :ex-data    (ex-data tx-exception)
                      :tx-id      tx-id ;; for quickly removing from the queue
                      :server     (raft/this-server raft-state)}]
    ;; returns promise
    (raft/leader-new-command! config :tx-exception created-body)))
