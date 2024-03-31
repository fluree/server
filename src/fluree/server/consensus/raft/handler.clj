(ns fluree.server.consensus.raft.handler
  (:require [fluree.db.util.log :as log]
            [fluree.server.components.http :as-alias http-routes]
            [fluree.server.consensus.raft.handlers.create-ledger :as create-ledger]
            [fluree.server.consensus.raft.handlers.ledger-created :as ledger-created]
            [fluree.server.consensus.raft.handlers.new-commit :as new-commit]
            [fluree.server.consensus.raft.handlers.new-index-file :as new-index-file]
            [fluree.server.consensus.raft.handlers.tx-exception :as tx-exception]
            [fluree.server.consensus.raft.handlers.tx-queue :as tx-queue]))

(set! *warn-on-reflection* true)

(def default-routes
  {:ledger-create          {:summary         "Request to create a new ledger. Generates :ledger-created event when successful"
                            :parameters      {:ledger    ::http-routes/ledger
                                              :txn       ::http-routes/txn
                                              :size      pos-int?
                                              :instant   pos-int?}
                            :auth            {} ;; specific authorization logic to determine if action is allowed.
                            :produces-events [:ledger-created :new-index-file :new-index]
                            :handler         create-ledger/handler
                            :processor       create-ledger/processor}
   :ledger-created         {:summary    "A new ledger has been created by a ledger node, register."
                            :parameters {:ledger    ::http-routes/ledger
                                         :txn       ::http-routes/txn
                                         :size      pos-int?
                                         :instant   pos-int?}
                            :auth       {} ;; specific authorization logic to determine if action is allowed.
                            :handler    ledger-created/handler
                            :processor  ledger-created/broadcast!}
   :tx-queue               {:summary   "Queues a new transaction for a given ledger. If the transactor, processes transaction"
                            :produces-events [:new-commit :tx-exception]
                            :handler   tx-queue/handler
                            :processor tx-queue/processor}
   :new-commit             {:summary   "A transaction has been processed into a new commit. Broadcast to connected clients."
                            :handler   new-commit/handler
                            :processor new-commit/broadcast!}
   :tx-exception           {:summary "Any transaction that has an exception which is not recorded in a new commit.  Broadcast to connected clients."
                            :handler tx-exception/handler}
   :new-index-file         {:summary   "A new (pending) index file is created for an ongoing indexing process"
                            :handler   new-index-file/handler
                            :processor new-index-file/processor}

   ;; TODO - below are anticipated event types, but not yet implemented

   :new-index              {:summary "A new index point for a given ledger has been created"}
   :ledger-delete          {:summary "Delete a ledger request"}
   :ledger-reindex         {:summary "Request a ledger be reindexed"}
   :ledger-garbage-collect {:summary "Request that a ledger have garbage collection run on old indexes"}
   :server-register        {:summary "Register/renews a consensus server lease informing the network it is still available"}
   :server-assign          {:summary "New server assignments for ledgers put out by consensus leader."}
   :server-add             {:summary "Attempts to add a new consensus server."}
   :server-remove          {:summary "Attempts to remove an existing consensus server"}})

(def default-event-handlers
  {:fluree/ledger-create      {:summary "A new ledger create request was just received."}
   :fluree/ledger-created     {:summary    "A new ledger was just created."
                               :broadcater :TODO}
   :fluree/commit-created     {:summary    "A new commit for a specific ledger is ready."
                               :broadcater :TODO}
   :fluree/index-created      {:summary    "A new index for a specific ledger is created and ready."
                               :broadcater :TODO}
   :fluree/tx-exception       {:summary    "A transaction exception where a new commit is not created."
                               :broadcater :TODO}

   :consensus/leader-change   {:summary "A notification from the raft protocol that a leader change has happened."
                               :handler :TODO}
   :consensus/server-add      {:summary ""
                               :handler :TODO}
   :consensus/server-remove   {:summary ""
                               :handler :TODO}
   :consensus/server-reassign {:summary "New worker assignments"}})

(defn unhandled-route-exception
  "Handles any exceptions not handled directly by the respective handlers."
  [event parameters message cause]
  (let [msg (or message
                (str "Consensus event handler unexpected exception for event " event
                     " caused by: " (ex-message cause)))]
    (log/error cause msg)
    (throw (ex-info msg {:status     500
                         :error      :node/consensus
                         :event      event
                         :parameters parameters}))))

(defn do-handler!
  "Executes the handler with a try/catch"
  [event handler-fn config params]
  (try
    (handler-fn config params)
    (catch Exception e
      (unhandled-route-exception event
                                 params
                                 (str "Unexpected error in handler function for event " event
                                      " with message: " (ex-message e))
                                 e))))

(defn do-processor!
  [event processor-fn config handler-response]
  (log/debug "Executing consensus processor fn:" event handler-response)
  (try
    (processor-fn config handler-response)
    handler-response
    (catch Exception e
      (unhandled-route-exception event
                                 handler-response
                                 (str "Unexpected error in processor function for event " event
                                      " with message: " (ex-message e))
                                 e))))

(defn create-handler
  [routes]
  (fn [config event parameters]
    (log/debug "New consensus event:" event parameters)
    (if-let [{:keys [handler processor]} (get routes event)]
      (if handler
        (let [handler-resp (do-handler! event handler config parameters)]
          (when processor
            (future
              ;; processor and broadcaster handled in a background thread, not part of return response
              (do-processor! event processor config handler-resp)))
          handler-resp)
        (unhandled-route-exception event parameters nil
                                   (Exception. (str "Handler function missing for event: " event))))
      (unhandled-route-exception event parameters nil
                                 (Exception. (str "Unknown consensus event received: " event))))))
