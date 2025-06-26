(ns fluree.server.consensus.raft.producers.tx-exception
  (:require [fluree.server.consensus.events :as events]
            [fluree.server.consensus.raft.participant :as participant]))

(defn consensus-push-tx-exception
  [{:keys [consensus/raft-state] :as config} {:keys [ledger-id tx-id]} tx-exception]
  (let [server    (participant/this-server raft-state)
        error-msg (events/error ledger-id tx-exception :server server :tx-id tx-id)]
    ;; returns promise
    (participant/leader-new-command! config :tx-exception error-msg)))
