(ns fluree.server.consensus.events
  "Common namespace for defining consensus event messages shared across consensus
  protocols")

(defn event-type
  [event]
  (:type event))

(defn create-ledger
  "Create a new event message to create a new ledger"
  [ledger-id tx-id txn opts]
  {:type      :ledger-create
   :txn       txn
   :size      (count txn)
   :tx-id     tx-id
   :ledger-id ledger-id
   :opts      opts
   :instant   (System/currentTimeMillis)})

(defn commit-transaction
  "Create a new event message to commit a new transaction"
  [ledger-id tx-id txn opts]
  {:type      :tx-queue
   :txn       txn
   :size      (count txn)
   :tx-id     tx-id
   :ledger-id ledger-id
   :opts      opts
   :instant   (System/currentTimeMillis)})

(defn transaction-committed
  "Post-transaction, the message we will broadcast out and/or deliver
  to a client awaiting a response."
  ([ledger-id tx-id {:keys [db address] :as _commit-result}]
   {:type      :transaction-committed
    :ledger-id ledger-id
    :t         (:t db)
    :tx-id     tx-id
    :commit    address})
  ([processing-server ledger-id tx-id commit-result]
   (-> (transaction-committed ledger-id tx-id commit-result)
       (assoc :server processing-server))))

(defn ledger-created
  ([ledger-id tx-id commit-result]
   (-> (transaction-committed ledger-id tx-id commit-result)
       (assoc :type :ledger-created)))
  ([processing-server ledger-id tx-id commit-result]
   (-> (ledger-created ledger-id tx-id commit-result)
       (assoc :server processing-server))))

(defn error
  ([ledger-id tx-id exception]
   (-> {:type          :error
        :ledger-id     ledger-id
        :tx-id         tx-id
        :error         exception
        :error-message (ex-message exception)
        :error-data    (ex-data exception)}))
  ([processing-server ledger-id tx-id exception]
   (-> (error ledger-id tx-id exception)
       (assoc :server processing-server))))

(defn error?
  [evt]
  (-> evt :type (= :error)))
