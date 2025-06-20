(ns fluree.server.consensus
  "To allow for pluggable consensus, we have a Transactor protocol. In order to allow
  for a new consensus type, we need to create a record with all of the following
  methods. Currently, we support Raft and Standalone."
  (:require [clojure.core.async :as async :refer [go <!]]
            [fluree.db.util.core :as util]
            [fluree.db.util.log :as log]
            [fluree.server.consensus.events :as events]
            [fluree.server.watcher :as watcher]))

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

(defn monitor-consensus
  [watcher ledger-id resp-chan & {:keys [tx-id]}]
  (go
    (let [resp (<! resp-chan)]
      ;; check for exception from consensus consensus, if so we must
      ;; deliver the watch here, but if successful the consensus process will
      ;; deliver the watch downstream
      (when (util/exception? resp)
        (log/warn resp "Error submitting event.")
        (let [error-event (events/error ledger-id resp :tx-id tx-id)]
          (watcher/deliver-event watcher (or tx-id ledger-id) error-event))))))
