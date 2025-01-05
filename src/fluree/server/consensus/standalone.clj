(ns fluree.server.consensus.standalone
  (:require [clojure.core.async :as async :refer [<! >! go go-loop]]
            [fluree.db.api :as fluree]
            [fluree.db.constants :as const]
            [fluree.db.util.async :refer [go-try]]
            [fluree.db.util.core :refer [exception? get-first-value]]
            [fluree.db.util.log :as log]
            [fluree.server.consensus :as consensus]
            [fluree.server.consensus.broadcast :as broadcast]
            [fluree.server.consensus.events :as events]
            [fluree.server.handlers.shared :refer [deref!]]))

(set! *warn-on-reflection* true)

(defn parse-opts
  "Extract the opts from the transaction and keywordify the top level keys."
  [expanded-txn]
  (let [string-opts (get-first-value expanded-txn const/iri-opts)]
    (reduce-kv (fn [opts k v]
                 (assoc opts (keyword k) v))
               {} string-opts)))

(defn create-ledger!
  [conn subscriptions watcher {:keys [ledger-id txn opts] :as params}]
  (go-try
    (let [create-opts   (parse-opts txn)
          ledger        (deref!
                         (fluree/create conn ledger-id create-opts))
          staged-db     (-> ledger
                            fluree/db
                            (fluree/stage txn opts)
                            deref!)
          commit-result (deref!
                         ;; following uses :file-data? and will return map with {:keys [db data-file commit-file]}
                         (fluree/commit! ledger staged-db {:file-data? true}))
          broadcast-msg (events/transaction-committed params commit-result)]
      (broadcast/announce-new-ledger! subscriptions watcher broadcast-msg))))

(defn transact!
  [conn subscriptions watcher {:keys [ledger-id tx-id txn opts] :as params}]
  (go-try
    (let [start-time    (System/currentTimeMillis)
          _             (log/debug "Starting transaction processing for ledger:" ledger-id
                                   "with tx-id" tx-id ". Transaction sat in queue for"
                                   (- start-time (:instant params)) "milliseconds.")
          ledger        (if (deref! (fluree/exists? conn ledger-id))
                          (deref! (fluree/load conn ledger-id))
                          (throw (ex-info "Ledger does not exist" {:ledger ledger-id})))
          staged-db     (-> ledger
                            fluree/db
                            (fluree/stage txn opts)
                            deref!)
          commit-result (deref!
                         (fluree/commit! ledger staged-db {:file-data? true}))
          broadcast-msg (events/transaction-committed params commit-result)]
      (broadcast/announce-new-commit! subscriptions watcher broadcast-msg))))

(defn process-event
  [conn subscriptions watcher event]
  (go
    (try
      (let [event-type (events/event-type event)
            event-msg  (events/event-data event)
            result     (<! (case event-type
                             :ledger-create (create-ledger! conn subscriptions watcher event-msg)
                             :tx-queue      (transact! conn subscriptions watcher event-msg)))]
        (if (exception? result)
          (let [error-msg (events/error event-msg result)]
            (broadcast/announce-error! watcher error-msg))
          result))
      (catch Exception e
        (log/error "Unexpected event message - expected two-tuple of [event-type event-data], "
                   "and of a supported event type. Received:" event e)))))

(defn new-transaction-queue
  ([conn subscriptions watcher]
   (new-transaction-queue conn subscriptions watcher nil))
  ([conn subscriptions watcher max-pending-txns]
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
                 result         (<! (process-event conn subscriptions watcher event))
                 i*             (inc i)]
             (log/trace "Processed transaction #" i* ". Result:" result)
             (async/put! out-ch result (fn [closed?]
                                         (when-not closed?
                                           (async/close! out-ch))))
             (recur i*)))))
     tx-queue)))

(defn put-and-close!
  [ch x]
  (doto ch
    (async/put! x)
    async/close!))

(def overloaded-error
  (ex-info "Too many pending transactions. Please try again later."
           {:status 503, :error :db/pending-transaction-limit}))

(defrecord BufferedTransactor [tx-queue]
  consensus/Transactor
  (-queue-new-ledger [_ new-ledger-event]
    (let [out-ch (async/chan)]
      (go (or (async/offer! tx-queue [new-ledger-event out-ch])
              (put-and-close! out-ch overloaded-error)))
      out-ch))

  (-queue-new-transaction [_ new-txn-event]
    (let [out-ch (async/chan)]
      (go (or (async/offer! tx-queue [new-txn-event out-ch])
              (put-and-close! out-ch overloaded-error)))
      out-ch)))

(defn start-buffered
  [conn subscriptions watcher max-pending-txns]
  (let [tx-queue (new-transaction-queue conn subscriptions watcher max-pending-txns)]
    (->BufferedTransactor tx-queue)))

(defrecord SynchronizedTransactor [tx-queue]
  consensus/Transactor
  (-queue-new-ledger [_ new-ledger-event]
    (let [out-ch (async/chan)]
      (go (>! tx-queue [new-ledger-event out-ch]))
      out-ch))

  (-queue-new-transaction [_ new-txn-event]
    (let [out-ch (async/chan)]
      (go (>! tx-queue [new-txn-event out-ch]))
      out-ch)))

(defn start-synchronized
  [conn subscriptions watcher]
  (let [tx-queue (new-transaction-queue conn subscriptions watcher)]
    (->SynchronizedTransactor tx-queue)))

(defn start
  ([conn subscriptions watcher]
   (start-synchronized conn subscriptions watcher))
  ([conn subscriptions watcher max-pending-txns]
   (if max-pending-txns
     (start-buffered conn subscriptions watcher max-pending-txns)
     (start-synchronized conn subscriptions watcher))))

(defn stop
  [{:keys [tx-queue] :as _transactor}]
  (async/close! tx-queue))
