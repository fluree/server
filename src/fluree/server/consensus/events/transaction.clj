(ns fluree.server.consensus.events.transaction
  (:require [fluree.db.connection :as connection]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.log :as log]))

(defn get-txn
  "Gets the transaction value from the event message `evt`."
  [evt]
  (:txn evt))

(defn save-txn!
  [conn event]
  (go-try
    (if-let [txn (get-txn event)]
      (let [{:keys [ledger-id]} event
            {:keys [address]}   (<? (connection/save-txn! conn ledger-id txn))]
        (-> event
            (assoc :txn-address address)
            (dissoc :txn)))
      (do (log/warn "Error saving transaction. No transaction found for event" event)
          event))))

(defn resolve-txn?
  [event]
  (and (:txn-address event)
       (not (:txn event))))

(defn resolve-txn
  [conn event]
  (go-try
    (if-let [address (:txn-address event)]
      (let [txn (<? (connection/resolve-txn conn address))]
        (-> event
            (assoc :txn txn)
            (dissoc :txn-address)))
      (do (log/warn "Error resolving transaction. No transaction address found for event" event)
          event))))
