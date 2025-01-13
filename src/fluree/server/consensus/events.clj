(ns fluree.server.consensus.events
  "Common namespace for defining consensus event messages shared across consensus
  protocols")

(defn event-type
  [event]
  (nth event 0))

(defn event-data
  [event]
  (nth event 1))

(defn create-ledger
  "Upon receiving a request to create a new ledger, an event
  message must be queued into the consensus state machine.

  Format is [event-name event-body]"
  [ledger-id tx-id txn opts]
  [:ledger-create {:txn       txn
                   :size      (count txn)
                   :tx-id     tx-id
                   :ledger-id ledger-id
                   :opts      opts
                   :instant   (System/currentTimeMillis)}])

(defn commit-transaction
  "Upon receiving a request to create a new ledger, an event
  message must be queued into the consensus state machine.

  Format is [event-name event-body]"
  [ledger-id tx-id txn opts]
  [:tx-queue {:txn       txn
              :size      (count txn)
              :tx-id     tx-id
              :ledger-id ledger-id
              :opts      opts
              :instant   (System/currentTimeMillis)}])

(defn transaction-committed
  "Post-transaction, the message we will broadcast out and/or deliver
  to a client awaiting a response."
  ([{:keys [ledger-id tx-id] :as _event-params}
    {:keys [db address] :as _commit-result}]
   {:ledger-id ledger-id
    :t         (:t db)
    :tx-id     tx-id
    :commit    address})
  ([processing-server event-params commit-result]
   (-> (transaction-committed event-params commit-result)
       (assoc :server processing-server))))

(defn error
  ([params exception]
   (-> params
       (select-keys [:ledger-id :tx-id])
       (assoc :ex-message (ex-message exception)
              :ex-data    (ex-data exception))))
  ([processing-server params exception]
   (-> (error params exception)
       (assoc :server processing-server))))
