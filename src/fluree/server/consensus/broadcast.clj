(ns fluree.server.consensus.broadcast
  (:require [fluree.db.util.log :as log]
            [fluree.server.consensus.subscriptions :as subscriptions]
            [fluree.server.consensus.watcher :as watcher]))

(set! *warn-on-reflection* true)

(defn announce-new-ledger!
  [subscriptions watcher ledger-created-event]
  (let [{:keys [ledger-id t commit server tx-id]} ledger-created-event]
    (log/info (str "New Ledger successfully created by server " server
                   ": " ledger-id " with tx-id: " tx-id "."))
    (watcher/deliver-commit watcher tx-id ledger-id t commit)
    (subscriptions/publicize-new-ledger subscriptions ledger-id commit)
    ::new-ledger))

(defn announce-new-commit!
  [subscriptions watcher transaction-commited-event]
  (let [{:keys [ledger-id t commit tx-id server]} transaction-commited-event]
    (log/info "New transaction completed for" ledger-id
              "tx-id: " tx-id "by server:" server)
    (watcher/deliver-commit watcher tx-id ledger-id t commit)
    (subscriptions/publicize-commit subscriptions ledger-id commit)
    ::new-commit))

(defn announce-error!
  [watcher error-event]
  (let [{:keys [tx-id ex-message ex-data]} error-event]
    (log/debug "Delivering tx-exception to watcher with msg/data: " ex-message ex-data)
    (watcher/deliver-error watcher tx-id (ex-info ex-message ex-data))
    ::error))
