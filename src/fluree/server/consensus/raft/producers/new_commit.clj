(ns fluree.server.consensus.raft.producers.new-commit
  (:require [fluree.server.consensus.messages :as messages]
            [fluree.server.consensus.raft.participant :as participant]))

(set! *warn-on-reflection* true)

(defn consensus-push-commit
  "Pushes a new commit through consensus.

  This is called with new commits immediately after processing a transaction.

  Returns promise that will have the eventual response once committed."
  [{:keys [consensus/raft-state] :as config} params commit-result]
  (let [created-body (messages/new-commit
                      (participant/this-server raft-state)
                      params commit-result)]

    ;; returns promise
    (participant/leader-new-command! config :new-commit created-body)))

;; TODO - the return signature of updated commits from indexing is different than
;; TODO - the return values of commit! API, which the above function handles.
;; TODO - Ideally these two functions can be consolidated once the inputs match
(defn consensus-push-index-commit
  "Pushes a new commit through consensus.

  This is called with updated commits generated from the indexing process.

  Returns promise that will have the eventual response once committed."
  [config {:keys [commit-res db] :as _params}]
  (consensus-push-commit config
                         {:ledger-id (:alias db)}
                         {:db                db
                          :commit-file-meta  commit-res}))
