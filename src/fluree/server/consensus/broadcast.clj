(ns fluree.server.consensus.broadcast
  (:require [fluree.db.util.log :as log]
            [fluree.server.consensus.subscriptions :as subscriptions]
            [fluree.server.consensus.watcher :as watcher]))

(set! *warn-on-reflection* true)

(defn announce-new-ledger!
  [subscriptions watcher ledger-created-event]
  (let [{:keys [ledger-id server tx-id commit-file-meta]} ledger-created-event]
    (log/info (str "New Ledger successfully created by server " server
                   ": " ledger-id " with tx-id: " tx-id "."))
    (watcher/deliver-watch watcher tx-id ledger-created-event)
    (subscriptions/send-message-to-all subscriptions "ledger-created" ledger-id (:json commit-file-meta))
    ::new-ledger))

(defn announce-new-commit!
  [subscriptions watcher transaction-commited-event]
  (let [{:keys [ledger-id tx-id server commit-file-meta]} transaction-commited-event]
    (log/info "New transaction completed for" ledger-id
              "tx-id: " tx-id "by server:" server)
    (watcher/deliver-watch watcher tx-id transaction-commited-event)
    (subscriptions/send-message-to-all subscriptions "new-commit" ledger-id
                                       (:json commit-file-meta))
    ::new-commit))

(defn announce-error!
  [watcher error-event]
  (let [{:keys [tx-id ex-message ex-data]} error-event]
    (log/debug "Delivering tx-exception to watcher with msg/data: " ex-message ex-data)
    (watcher/deliver-watch watcher tx-id (ex-info ex-message ex-data))
    ::error))
