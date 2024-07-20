(ns fluree.server.consensus
  "To allow for pluggable consensus, we have a TxGroup protocol. In order to allow
  for a new consensus type, we need to create a record with all of the following
  methods. Currently, we support Raft and Standalone."
  (:require [fluree.db.util.log :as log]
            [fluree.server.subscriptions :as subscriptions]
            [fluree.server.watcher :as watcher]))

(set! *warn-on-reflection* true)

(defprotocol TxGroup
  (queue-new-ledger [group ledger-id tx-id txn opts])
  (queue-new-transaction [group ledger-id tx-id txn opts]))

(defn broadcast-new-ledger!
  [subscriptions watcher new-ledger-result]
  (let [{:keys [ledger-id server tx-id commit-file-meta]} new-ledger-result]
    (log/info (str "New Ledger successfully created by server " server
                   ": " ledger-id " with tx-id: " tx-id "."))
    (watcher/deliver-watch watcher tx-id new-ledger-result)
    (subscriptions/send-message-to-all subscriptions "ledger-created" ledger-id (:json commit-file-meta))
    :success))

(defn broadcast-new-commit!
  [subscriptions watcher commit-result]
  (let [{:keys [ledger-id tx-id server commit-file-meta]} commit-result]
    (log/info "New transaction completed for" ledger-id
              "tx-id: " tx-id "by server:" server)
    (watcher/deliver-watch watcher tx-id commit-result)
    (subscriptions/send-message-to-all subscriptions "new-commit" ledger-id
                                       (:json commit-file-meta))
    :success))

(defn broadcast-error!
  [watcher error-meta]
  (let [{:keys [tx-id ex-message ex-data]} error-meta]
    (log/debug "Delivering tx-exception to watcher with msg/data: " ex-message ex-data)
    (watcher/deliver-watch watcher tx-id (ex-info ex-message ex-data))))
