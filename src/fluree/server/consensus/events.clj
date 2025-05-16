(ns fluree.server.consensus.events
  "Common namespace for defining consensus event messages shared across consensus
  protocols"
  (:require [fluree.db.connection :as connection]
            [fluree.db.track :as-alias track]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.log :as log]))

(defn event-type
  [event]
  (:type event))

(defn type?
  [evt type]
  (-> evt event-type (= type)))

(defn txn-address?
  [x]
  (string? x))

(defn txn?
  [x]
  (map? x))

(defn with-txn
  [evt txn]
  (cond (txn-address? txn)
        (assoc evt :txn-address txn)

        (txn? evt)
        (assoc evt :txn txn)

        :else
        (throw (ex-info "Unrecognized transaction format"
                        {:status 400, :error :server/unrecognized-transaction}))))

(defn create-ledger
  "Create a new event message to create a new ledger. The `txn` argument may
  either be a transaction document map or an address for where the document is
  stored"
  [ledger-id tx-id txn opts]
  (let [evt {:type      :ledger-create
             :tx-id     tx-id
             :ledger-id ledger-id
             :opts      opts
             :instant   (System/currentTimeMillis)}]
    (with-txn evt txn)))

(defn commit-transaction
  "Create a new event message to commit a new transaction. The `txn` argument may
  either be a transaction document map or an address for where the document is
  stored"
  [ledger-id tx-id txn opts]
  (let [evt {:type      :tx-queue
             :tx-id     tx-id
             :ledger-id ledger-id
             :opts      opts
             :instant   (System/currentTimeMillis)}]
    (with-txn evt txn)))

(defn get-txn
  "Gets the transaction value, either a transaction document or the storage
  address for the transaction from the event message `evt`."
  [evt]
  (or (:txn evt)
      (:txn-address evt)))

(defn raw-txn-opt?
  [event]
  (-> event :opts :raw-txn some?))

(defn save-raw-txn!
  "Saves the value of the `:raw-txn` option found within the event message `event`
  to the commit storage accessible to the connection `conn`. Returns a new event
  message with the `:raw-txn` option replaced by the address the document was
  stored under."
  [conn event]
  (go-try
    (if-let [raw-txn (-> event :opts :raw-txn)]
      (let [{:keys [ledger-id]} event
            {:keys [address]}   (<? (connection/save-txn! conn ledger-id raw-txn))]
        (-> event
            (assoc-in [:opts :raw-txn-address] address)
            (update :opts dissoc :raw-txn)))
      event)))

(defn save-txns!
  "Saves the transaction documents found within the event message `event` to the
  commit storage accessible to the connection `conn`. Returns a new event
  message with the transaction documents replaced by the address the document
  was stored under."
  [conn event]
  (go-try
    (if-let [txn (:txn event)]
      (let [{:keys [ledger-id]} event
            {:keys [address]}   (<? (connection/save-txn! conn ledger-id txn))
            event* (-> event
                       (assoc :txn-address address)
                       (dissoc :txn))]
        (if (raw-txn-opt? event*)
          (<? (save-raw-txn! conn event*))
          event*))
      (do (when-not (:txn-address event)
            (log/warn "Error saving transaction. No transaction found for event" event))
          event))))

(defn resolve-txn?
  "Returns true if `event` only contains an address reference to a transaction
  document without the document itself."
  [event]
  (and (:txn-address event)
       (not (:txn event))))

(defn resolve-raw-txn-opt?
  [event]
  (-> event :opts :raw-txn-address some?))

(defn resolve-raw-txn
  [conn event]
  (go-try
    (if-let [address (-> event :opts :raw-txn-address)]
      (let [raw-txn (<? (connection/resolve-txn conn address))]
        (-> event
            (assoc-in [:opts :raw-txn] raw-txn)
            (update :opts dissoc :raw-txn-address)))
      event)))

(defn resolve-txns
  [conn event]
  (go-try
    (if-let [address (:txn-address event)]
      (let [txn    (<? (connection/resolve-txn conn address))
            event* (-> event
                       (assoc :txn txn)
                       (dissoc :txn-address))]
        (if (resolve-raw-txn-opt? event*)
          (<? (resolve-raw-txn conn event*))
          event*))
      (do (log/warn "Error resolving transaction. No transaction address found for event" event)
          event))))

(defn transaction-committed
  "Post-transaction, the message we will broadcast out and/or deliver
  to a client awaiting a response."
  ([ledger-id tx-id {:keys [db address hash fuel policy] :as _commit-result}]
   (cond-> {:type      :transaction-committed
            :ledger-id ledger-id
            :t         (:t db)
            :tx-id     tx-id
            :commit    {:address address
                        :hash    hash}}
     fuel   (assoc :fuel fuel)
     policy (assoc :policy policy)))
  ([processing-server ledger-id tx-id commit-result]
   (-> (transaction-committed ledger-id tx-id commit-result)
       (assoc :server processing-server))))

(defn transaction-committed?
  [evt]
  (type? evt :transaction-committed))

(defn ledger-created
  ([ledger-id tx-id commit-result]
   (-> (transaction-committed ledger-id tx-id commit-result)
       (assoc :type :ledger-created)))
  ([processing-server ledger-id tx-id commit-result]
   (-> (ledger-created ledger-id tx-id commit-result)
       (assoc :server processing-server))))

(defn ledger-created?
  [evt]
  (type? evt :ledger-created))

(defn error
  ([ledger-id tx-id exception]
   (-> {:type          :error
        :ledger-id     ledger-id
        :tx-id         tx-id
        :error-message (ex-message exception)
        :error-data    (ex-data exception)}))
  ([processing-server ledger-id tx-id exception]
   (-> (error ledger-id tx-id exception)
       (assoc :server processing-server))))

(defn error?
  [evt]
  (type? evt :error))

(defn outcome?
  [evt]
  (or (transaction-committed? evt)
      (ledger-created? evt)
      (error? evt)))
