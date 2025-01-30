(ns fluree.server.consensus.broadcast
  (:require [fluree.db.util.log :as log]
            [fluree.server.consensus.events :as events]
            [fluree.server.consensus.watcher :as watcher]))

(set! *warn-on-reflection* true)

(defprotocol Broadcaster
  (-broadcast [b ledger-id event]))

(defn broadcast-new-ledger!
  [subscriptions watcher new-ledger-params new-ledger-result]
  (let [{:keys [ledger-id t commit server tx-id] :as ledger-created-event}
        (events/ledger-created new-ledger-params new-ledger-result)]
    (log/info (str "New Ledger successfully created by server " server
                   ": " ledger-id " with tx-id: " tx-id "."))
    (watcher/deliver-commit watcher tx-id ledger-id t commit)
    (-broadcast subscriptions ledger-id ledger-created-event)
    ::new-ledger))

(defn broadcast-new-commit!
  [subscriptions watcher commit-params commit-result]
  (let [{:keys [ledger-id t commit tx-id server] :as transaction-commited-event}
        (events/transaction-committed commit-params commit-result)]
    (log/info "New transaction completed for" ledger-id
              "tx-id: " tx-id "by server:" server)
    (watcher/deliver-commit watcher tx-id ledger-id t commit)
    (-broadcast subscriptions ledger-id transaction-commited-event)
    ::new-commit))

(defn broadcast-error!
  [watcher error-event]
  (let [{:keys [tx-id ex-message ex-data]} error-event]
    (log/debug "Delivering tx-exception to watcher with msg/data: " ex-message ex-data)
    (watcher/deliver-error watcher tx-id (ex-info ex-message ex-data))
    ::error))
