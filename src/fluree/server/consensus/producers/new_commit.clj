(ns fluree.server.consensus.producers.new-commit
  (:require [fluree.db.ledger.proto :as ledger-proto]
            [fluree.server.consensus.core :as consensus]
            [fluree.server.consensus.raft.core :as raft]))

(set! *warn-on-reflection* true)

(defn consensus-push-commit
  "Pushes a new commit through consensus.

  This is called with new commits immediately after processing a transaction.

  Returns promise that will have the eventual response once committed."
  [{:keys [:consensus/state-atom :consensus/raft-state :fluree/conn] :as config}
   {:keys [ledger-id tx-id] :as _params}
   {:keys [db data-file-meta commit-file-meta context-file-meta]}]
  (let [created-body {:ledger-id         ledger-id
                      :data-file-meta    data-file-meta
                      :commit-file-meta  commit-file-meta
                      :context-file-meta context-file-meta
                      ;; below is metadata for quickly validating into the state machine, not retained
                      :t                 (:t db) ;; for quickly validating this is the next 'block'
                      :tx-id             tx-id ;; for quickly removing from the queue
                      :server            (consensus/this-server raft-state) ;; for quickly ensuring this server *is* still the leader
                      }]
    ;; returns promise
    (raft/leader-new-command! config :new-commit created-body)))

;; TODO - the return signature of updated commits from indexing is different than
;; TODO - the return values of commit! API, which the above function handles.
;; TODO - Ideally these two functions can be consolidated once the inputs match
(defn consensus-push-index-commit
  "Pushes a new commit through consensus.

  This is called with updated commits generated from the indexing process.

  Returns promise that will have the eventual response once committed."
  [{:keys [:consensus/state-atom :consensus/raft-state :fluree/conn] :as config}
   {:keys [commit-res context-res db] :as _params}]
  (let [ledger    (:ledger db)
        ledger-id (ledger-proto/-alias ledger)]
    (consensus-push-commit config
                           {:ledger-id ledger-id}
                           {:db                db
                            :context-file-meta context-res
                            :commit-file-meta  commit-res})))