(ns fluree.server.consensus
  "To allow for pluggable consensus, we have a Transactor protocol. In order to allow
  for a new consensus type, we need to create a record with all of the following
  methods. Currently, we support Raft and Standalone."
  (:require [fluree.db.util.log :as log]
            [fluree.server.consensus.events :as events]
            [steffan-westcott.clj-otel.api.trace.span :as span]))

(set! *warn-on-reflection* true)

(defprotocol Transactor
  (-queue-new-ledger [transactor new-ledger-params])
  (-queue-new-transaction [transactor new-tx-params]))

(defn queue-new-ledger
  [transactor ledger-id tx-id txn opts]
  (log/trace "queue-new-ledger:" ledger-id tx-id txn)
  (span/with-span-binding [ctx {:name "Queue New Ledger"
                                :kind :producer
                                :attributes {"ledger.id" ledger-id
                                             "transaction.id" tx-id}}]
    (let [event-params (events/create-ledger ledger-id tx-id txn opts)]
      (-queue-new-ledger transactor (with-meta event-params {:otel-context ctx})))))

(defn queue-new-transaction
  [transactor ledger-id tx-id txn opts]
  (log/trace "queue-new-transaction:" txn)
  (span/with-span-binding [ctx {:name "Queue New Transaction"
                                :kind :producer
                                :attributes {"ledger.id" ledger-id
                                             "transaction.id" tx-id}}]
    (let [event-params (events/commit-transaction ledger-id tx-id txn opts)]
      (-queue-new-transaction transactor (with-meta event-params {:otel-context ctx})))))
