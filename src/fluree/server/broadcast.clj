(ns fluree.server.broadcast
  (:require [fluree.server.consensus.events :as events]))

(set! *warn-on-reflection* true)

(defprotocol Broadcaster
  (-broadcast-commit [b ledger-id event])
  (-broadcast-error [b ledger-id error-event])
  (-broadcast-drop [b ledger-id event]))

(defn broadcast-new-ledger!
  [broadcaster {:keys [ledger-id] :as ledger-created-event}]
  (-broadcast-commit broadcaster ledger-id ledger-created-event)
  ::new-ledger)

(defn broadcast-dropped-ledger!
  [broadcaster {:keys [ledger-id] :as ledger-dropped-event}]
  (-broadcast-drop broadcaster ledger-id ledger-dropped-event)
  ::drop-ledger)

(defn broadcast-new-commit!
  [broadcaster {:keys [ledger-id] :as transaction-committed-event}]
  (-broadcast-commit broadcaster ledger-id transaction-committed-event)
  ::new-commit)

(defn broadcast-error!
  [broadcaster {:keys [ledger-id] :as error-event}]
  (-broadcast-error broadcaster ledger-id error-event)
  ::error)

(defn broadcast-event!
  [broadcaster result]
  (case (events/event-type result)
    :transaction-committed (broadcast-new-commit! broadcaster result)
    :ledger-created        (broadcast-new-ledger! broadcaster result)
    :ledger-dropped        (broadcast-dropped-ledger! broadcaster result)
    :error                 (broadcast-error! broadcaster result)))
