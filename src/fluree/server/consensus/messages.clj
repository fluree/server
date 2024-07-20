(ns fluree.server.consensus.messages
  "Common namespace for defining consensus messages shared across consensus
  protocols")

(defn queue-new-ledger
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

(defn new-ledger
  ([{:keys [ledger-id tx-id] :as _event-params}
    commit-result]
   (-> commit-result
       (select-keys [:db :data-file-meta :commit-file-meta])
       (assoc :tx-id tx-id
              :ledger-id ledger-id
              :t 1)))
  ([processing-server event-params commit-result]
   (-> (new-ledger event-params commit-result)
       (assoc :server processing-server))))

(defn queue-new-transaction
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

(defn new-commit
  "Post-transaction, the message we will broadcast out and/or deliver
  to a client awaiting a response."
  ([{:keys [ledger-id tx-id] :as _event-params}
    {:keys [db data-file-meta commit-file-meta] :as _commit-result}]
   {:ledger-id        ledger-id
    :data-file-meta   data-file-meta
    :commit-file-meta commit-file-meta
    ;; below is metadata for quickly validating into the state machine, not retained
    :t                (:t db) ;; for quickly validating this is the next 'block'
    :tx-id            tx-id ;; for quickly removing from the queue
    })
  ([processing-server event-params commit-result]
   (-> (new-commit event-params commit-result)
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
