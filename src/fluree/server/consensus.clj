(ns fluree.server.consensus
  "To allow for pluggable consensus, we have a TxGroup protocol. In order to allow
  for a new consensus type, we need to create a record with all of the following
  methods. Currently, we support Raft and Standalone."
  (:require [fluree.db.util.log :as log]
            [fluree.server.consensus.events :as events]))

(set! *warn-on-reflection* true)

(defprotocol TxGroup
  (-queue-new-ledger [group event-params])
  (-queue-new-transaction [group event-params]))

(defn queue-new-ledger
  [group ledger-id tx-id txn opts]
  (log/trace "queue-new-ledger:" ledger-id tx-id txn)
  (let [event-params (events/create-ledger ledger-id tx-id txn opts)]
    (-queue-new-ledger group event-params)))

(defn queue-new-transaction
  [group ledger-id tx-id txn opts]
  (log/trace "queue-new-transaction:" txn)
  (let [event-params (events/commit-transaction ledger-id tx-id txn opts)]
    (-queue-new-transaction group event-params)))
