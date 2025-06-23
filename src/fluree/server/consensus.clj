(ns fluree.server.consensus
  "To allow for pluggable consensus, we have a Transactor protocol. In order to allow
  for a new consensus type, we need to create a record with all of the following
  methods. Currently, we support Raft and Standalone."
  (:require [fluree.db.util.log :as log]
            [fluree.server.consensus.events :as events]))

(set! *warn-on-reflection* true)

(defprotocol Transactor
  (-queue-new-ledger [transactor new-ledger-params])
  (-queue-new-transaction [transactor new-tx-params])
  (-queue-drop-ledger [transactor drop-ledger-params]))

(defn queue-new-ledger
  [transactor ledger-id tx-id txn opts]
  (log/trace "queue-new-ledger:" ledger-id tx-id txn)
  (let [event-params (events/create-ledger ledger-id tx-id txn opts)]
    (-queue-new-ledger transactor event-params)))

(defn queue-new-transaction
  [transactor ledger-id tx-id txn opts]
  (log/trace "queue-new-transaction:" txn)
  (let [event-params (events/commit-transaction ledger-id tx-id txn opts)]
    (-queue-new-transaction transactor event-params)))

(defn queue-drop-ledger
  [transactor ledger-id]
  (let [event-params (events/drop-ledger ledger-id)]
    (-queue-drop-ledger transactor event-params)))
