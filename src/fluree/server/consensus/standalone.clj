(ns fluree.server.consensus.standalone
  (:require [clojure.core.async :as async :refer [<! >! go go-loop]]
            [fluree.db.api :as fluree]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.log :as log]
            [fluree.server.consensus :as consensus]
            [fluree.server.consensus.events :as events]
            [fluree.server.consensus.response :as response]
            [fluree.server.handlers.shared :refer [deref!]]
            [fluree.server.watcher :as watcher]
            [fluree.server.handlers.shared :refer [deref!]]
            [steffan-westcott.clj-otel.api.trace.span :as span]))

(set! *warn-on-reflection* true)

(defn ensure-file-reports
  [meta]
  (if (true? meta)
    meta
    (assoc meta :file true)))

(defn create-ledger!
  [conn watcher broadcaster {:keys [ledger-id tx-id txn opts] :as _params}]
  (go-try
    (let [opts*         (update opts :meta ensure-file-reports)
          commit-result (deref! (fluree/create-with-txn conn txn opts*))]
      (response/announce-new-ledger watcher broadcaster ledger-id tx-id commit-result))))

(defn transact!
  [conn watcher broadcaster {:keys [ledger-id tx-id txn opts] :as params}]
  (go-try
    (let [start-time    (System/currentTimeMillis)
          _             (log/debug "Starting transaction processing for ledger:" ledger-id
                                   "with tx-id" tx-id ". Transaction sat in queue for"
                                   (- start-time (:instant params)) "milliseconds.")
          opts*         (update opts :meta ensure-file-reports)
          commit-result (deref! (fluree/transact! conn txn opts*))]
      (response/announce-commit watcher broadcaster ledger-id tx-id commit-result))))

(defn process-event
  [conn watcher broadcaster event]
  (span/with-span!  {:name "fluree.server.consensus.standalone/process-event"
                      ;; should be :consumer but only :server supported by xray  https://github.com/aws-observability/aws-otel-collector/issues/1773
                     :span-kind :server
                     :parent (consensus/get-trace-context event)}
    (go
      (try
        (let [event*     (if (events/resolve-txn? event)
                           (<? (events/resolve-txns conn event))
                           event)
              event-type (events/event-type event*)]
          (cond
            (= :ledger-create event-type)
            (<? (create-ledger! conn watcher broadcaster event*))

            (= :tx-queue event-type)
            (<? (transact! conn watcher broadcaster event*))

            :else
            (throw (ex-info (str "Unexpected event message: event type '" event-type "' not"
                                 " one of (':ledger-create', ':tx-queue')")
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
