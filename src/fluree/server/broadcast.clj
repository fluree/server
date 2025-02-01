(ns fluree.server.broadcast
  (:require [fluree.db.util.log :as log]
            [fluree.server.consensus.events :as events]
            [fluree.server.consensus.watcher :as watcher]))

(set! *warn-on-reflection* true)

(defprotocol Broadcaster
  (-broadcast [b ledger-id event]))

(defn broadcast-new-ledger!
  [subscriptions watcher new-ledger-params new-ledger-result]
  (let [{:keys [ledger-id server tx-id] :as ledger-created-event}
        (events/ledger-created new-ledger-params new-ledger-result)]
    (log/info (str "New Ledger successfully created by server " server
                   ": " ledger-id " with tx-id: " tx-id "."))
    (watcher/deliver-commit watcher tx-id ledger-created-event)
    (-broadcast subscriptions ledger-id ledger-created-event)
    ::new-ledger))

(defn broadcast-new-commit!
  [subscriptions watcher commit-params commit-result]
  (let [{:keys [ledger-id tx-id server] :as transaction-committed-event}
        (events/transaction-committed commit-params commit-result)]
    (log/info "New transaction completed for" ledger-id
              "tx-id: " tx-id "by server:" server)
    (watcher/deliver-commit watcher tx-id transaction-committed-event)
    (-broadcast subscriptions ledger-id transaction-committed-event)
    ::new-commit))

(defn broadcast-error!
  [_subscriptions watcher event-msg error]
  (let [{:keys [tx-id]} event-msg]
    (log/debug error "Delivering tx-exception to watcher")
    (watcher/deliver-error watcher tx-id error)
    ::error))
