(ns fluree.server.broadcast)

(set! *warn-on-reflection* true)

(defprotocol Broadcaster
  (-broadcast-commit [b ledger-id event])
  (-broadcast-error [b ledger-id error-event]))

(defn broadcast-new-ledger!
  [broadcaster {:keys [ledger-id] :as ledger-created-event}]
  (-broadcast-commit broadcaster ledger-id ledger-created-event)
  ::new-ledger)

(defn broadcast-new-commit!
  [broadcaster {:keys [ledger-id] :as transaction-committed-event}]
  (-broadcast-commit broadcaster ledger-id transaction-committed-event)
  ::new-commit)

(defn broadcast-error!
  [broadcaster {:keys [ledger-id] :as error-event}]
  (-broadcast-error broadcaster ledger-id error-event)
  ::error)
