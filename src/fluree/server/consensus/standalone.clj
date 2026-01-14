(ns fluree.server.consensus.standalone
  (:require [clojure.core.async :as async :refer [<! >! go go-loop]]
            [fluree.db.api :as fluree]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.log :as log]
            [fluree.db.util.trace :as trace]
            [fluree.server.consensus :as consensus]
            [fluree.server.consensus.events :as events]
            [fluree.server.consensus.response :as response]
            [fluree.server.consensus.shared.create :as shared-create]
            [fluree.server.handlers.shared :refer [deref!]]))

(set! *warn-on-reflection* true)

(defn create-ledger!
  [conn watcher broadcaster {:keys [ledger-id tx-id txn opts otel/context] :as _params}]
  (go-try
    (let [commit-result
          (trace/form ::create-ledger! {:parent context}
                      (if txn
                        (deref! (fluree/create-with-txn conn txn opts))
                        (let [db (deref! (fluree/create conn ledger-id opts))]
                          (shared-create/genesis-result db))))]
      (response/announce-new-ledger watcher broadcaster ledger-id tx-id commit-result))))

(defn drop-ledger!
  [conn watcher broadcaster {:keys [ledger-id otel/context] :as _params}]
  (go-try
    (let [drop-result (trace/form ::drop-ledger! {:parent context}
                                  (deref! (fluree/drop conn ledger-id)))]
      (response/announce-dropped-ledger watcher broadcaster ledger-id drop-result))))

(defn transact!
  [conn watcher broadcaster {:keys [ledger-id tx-id txn opts otel/context]}]
  (go-try
    (let [commit-result (trace/form ::transact! {:parent context}
                                    (case (:op opts)
                                      :update (deref! (fluree/update! conn ledger-id txn opts))
                                      :upsert (deref! (fluree/upsert! conn ledger-id txn opts))
                                      :insert (deref! (fluree/insert! conn ledger-id txn opts))))]
      (response/announce-commit watcher broadcaster ledger-id tx-id commit-result))))

(defn process-event
  [conn watcher broadcaster event]
  (trace/with-parent-context ::process-event (:otel/context event)
    (go
      (try
        (let [event*     (if (events/resolve-txn? event)
                           (<? (events/resolve-txns conn event))
                           event)
              event-type (events/event-type event*)]
          (cond
            (= :ledger-create event-type)
            (<? (create-ledger! conn watcher broadcaster event*))

            (= :ledger-drop event-type)
            (<? (drop-ledger! conn watcher broadcaster event*))

            (= :tx-queue event-type)
            (<? (transact! conn watcher broadcaster event*))

            :else
            (throw (ex-info (str "Unexpected event message: event type '" event-type "' not"
                                 " one of (':ledger-create', ':ledger-drop', ':tx-queue')")
                            {:status 500, :error :consensus/unexpected-event}))))
        (catch Exception e
          (let [{:keys [ledger-id tx-id]} event]
            (log/warn e "Error processing consensus event")
            (response/announce-error watcher broadcaster ledger-id tx-id e)))))))

(defn error?
  [result]
  (or (= result ::error)
      (nil? result)))

(defn put-and-close!
  [ch x]
  (async/put! ch x (fn [closed?]
                     (when-not closed?
                       (async/close! ch)))))

(defn new-transaction-queue
  ([conn watcher broadcaster]
   (new-transaction-queue conn watcher broadcaster nil))
  ([conn watcher broadcaster max-pending-txns]
   (let [tx-queue (if (pos-int? max-pending-txns)
                    (async/chan max-pending-txns)
                    (async/chan))]
     (go-loop [i 0]
       (let [timeout-ch (async/timeout 5000)
             [input ch] (async/alts! [tx-queue timeout-ch])]
         (cond
           (= timeout-ch ch)
           (do
             (log/trace "No new transactions in 5 seconds."
                        "Processed" i "total transactions.")
             (recur i))

           (nil? input)
           (do
             (log/warn "Local synchronized transaction queue was closed."
                       "No new transactions will be processed.")
             ::closed)

           :else
           (let [[event out-ch] input
                 result         (<! (process-event conn watcher broadcaster event))
                 i*             (inc i)]
             (log/trace "Processed transaction #" i* ". Result:" result)
             (when-not (error? result)
               (put-and-close! out-ch result))
             (recur i*)))))
     tx-queue)))

(def overloaded-error
  (ex-info "Too many pending transactions. Please try again later."
           {:status 503, :error :db/pending-transaction-limit}))

(defrecord BufferedTransactor [watcher tx-queue]
  consensus/Transactor
  (-queue-new-ledger [_ new-ledger-event]
    (let [out-ch (async/chan)]
      (go (or (async/offer! tx-queue [new-ledger-event out-ch])
              (put-and-close! out-ch overloaded-error)))
      out-ch))

  (-queue-drop-ledger [_ drop-ledger-event]
    (let [out-ch (async/chan)]
      (go (or (async/offer! tx-queue [drop-ledger-event out-ch])
              (put-and-close! out-ch overloaded-error)))
      out-ch))

  (-queue-new-transaction [_ new-txn-event]
    (let [out-ch (async/chan)]
      (go (or (async/offer! tx-queue [new-txn-event out-ch])
              (put-and-close! out-ch overloaded-error)))
      out-ch)))

(defn start-buffered
  [conn watcher broadcaster max-pending-txns]
  (let [tx-queue (new-transaction-queue conn watcher broadcaster max-pending-txns)]
    (->BufferedTransactor watcher tx-queue)))

(defrecord SynchronizedTransactor [watcher tx-queue]
  consensus/Transactor
  (-queue-new-ledger [_ new-ledger-event]
    (let [out-ch (async/chan)]
      (go (>! tx-queue [new-ledger-event out-ch]))
      out-ch))

  (-queue-drop-ledger [_ drop-ledger-event]
    (let [out-ch (async/chan)]
      (go (>! tx-queue [drop-ledger-event out-ch]))
      out-ch))

  (-queue-new-transaction [_ new-txn-event]
    (let [out-ch (async/chan)]
      (go (>! tx-queue [new-txn-event out-ch]))
      out-ch)))

(defn start-synchronized
  [conn watcher broadcaster]
  (let [tx-queue (new-transaction-queue conn watcher broadcaster)]
    (->SynchronizedTransactor watcher tx-queue)))

(defn start
  ([conn watcher broadcaster]
   (start-synchronized conn watcher broadcaster))
  ([conn watcher broadcaster max-pending-txns]
   (if max-pending-txns
     (start-buffered conn watcher broadcaster max-pending-txns)
     (start-synchronized conn watcher broadcaster))))

(defn stop
  [{:keys [tx-queue] :as _transactor}]
  (async/close! tx-queue))
