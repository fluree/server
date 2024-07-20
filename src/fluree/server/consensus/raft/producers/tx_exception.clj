(ns fluree.server.consensus.raft.producers.tx-exception
  (:require [fluree.server.consensus.messages :as messages]
            [fluree.server.consensus.raft.participant :as participant]))

(defn consensus-push-tx-exception
  [{:keys [consensus/raft-state] :as config} params tx-exception]
  (let [server (participant/this-server raft-state)
        error-message (messages/error server params tx-exception)]
    ;; returns promise
    (participant/leader-new-command! config :tx-exception error-message)))
