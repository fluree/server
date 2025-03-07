(ns fluree.server.consensus
  "To allow for pluggable consensus, we have a Transactor protocol. In order to allow
  for a new consensus type, we need to create a record with all of the following
  methods. Currently, we support Raft and Standalone."
  (:require [clojure.string :as string]
            [fluree.db.util.log :as log]
            [fluree.server.consensus.events :as events]
            [steffan-westcott.clj-otel.api.trace.span :as span]
            [steffan-westcott.clj-otel.context :as otel-context]))

(set! *warn-on-reflection* true)

(defprotocol Transactor
  (-queue-new-ledger [transactor new-ledger-params])
  (-queue-new-transaction [transactor new-tx-params]))

(defn with-trace-context
  [event]
  (assoc event ::trace-context (otel-context/->headers)))

;; need to lowercase map keys before calling otel-context/headers->merged-context
;; https://github.com/steffan-westcott/clj-otel/issues/26
(defn ^:private lowercase-keys [m]
  (into {} (map (fn [[k v]] [(string/lower-case (name k)) v])) m))

(defn get-trace-context [event]
  (-> event ::trace-context lowercase-keys otel-context/headers->merged-context))

(defn queue-new-ledger
  [transactor ledger-id tx-id txn opts]
  (log/trace "queue-new-ledger:" ledger-id tx-id txn)
  (span/with-span! {:name "Consensus Queue New Ledger"
                    :kind :producer
                    :attributes {"ledger.id" ledger-id
                                 "transaction.id" tx-id}}
    (let [event-params (events/create-ledger ledger-id tx-id txn opts)]
      (-queue-new-ledger transactor (with-trace-context event-params)))))

(defn queue-new-transaction
  [transactor ledger-id tx-id txn opts]
  (log/trace "queue-new-transaction:" txn)
  (span/with-span!  {:name "Consensus Queue New Transaction"
                     :kind :producer
                     :attributes {"ledger.id" ledger-id
                                  "transaction.id" tx-id}}
    (let [event-params (events/commit-transaction ledger-id tx-id txn opts)]
      (-queue-new-transaction transactor (with-trace-context event-params)))))
