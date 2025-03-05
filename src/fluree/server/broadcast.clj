(ns fluree.server.broadcast)

(set! *warn-on-reflection* true)

(defprotocol Broadcaster
  (-broadcast [b ledger-id event]))

(defn broadcast-new-ledger!
  [broadcaster {:keys [ledger-id] :as ledger-created-event}]
  (-broadcast broadcaster ledger-id ledger-created-event)
  ::new-ledger)

(defn broadcast-new-commit!
  [broadcaster {:keys [ledger-id] :as transaction-committed-event}]
  (-broadcast broadcaster ledger-id transaction-committed-event)
  ::new-commit)

(defn broadcast-error!
  [broadcaster {:keys [ledger-id] :as error-event}]
  (-broadcast broadcaster ledger-id error-event)
  ::error)
