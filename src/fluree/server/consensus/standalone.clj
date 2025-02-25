(ns fluree.server.consensus.standalone
  (:require [clojure.core.async :as async :refer [<! >! go go-loop]]
            [fluree.db.api :as fluree]
            [fluree.db.constants :as const]
            [fluree.db.util.async :refer [go-try]]
            [fluree.db.util.core :refer [exception? get-first-value]]
            [fluree.db.util.log :as log]
            [fluree.server.broadcast :as broadcast]
            [fluree.server.consensus :as consensus]
            [fluree.server.consensus.events :as events]
            [fluree.server.handlers.shared :refer [deref!]]
            [steffan-westcott.clj-otel.api.trace.span :as span]
            [steffan-westcott.clj-otel.context :as otel-context])
  (:import [io.opentelemetry.api.trace Span]
           [org.slf4j MDC]))

(defn get-trace-id [ctx]
  (let [span (Span/fromContext ctx) ;; Get the span from the context
        span-context (.getSpanContext span)] ;; Extract the span context
    (if (.isValid span-context)
      (.getTraceId span-context) ;; Get the trace ID from SpanContext
      nil))) ;; Return nil if no valid trace
(defmacro with-log-attributes
  "Temporarily sets MDC (log attributes) for Logback logging within the body."
  [attr-map & body]
  `(let [attr-map# ~attr-map]
     (doseq [[k# v#] attr-map#]
       (org.slf4j.MDC/put (name k#) (str v#)))
     (do ~@body)
     (doseq [[k# v#] attr-map#]
       (org.slf4j.MDC/remove (name k#)))))

(set! *warn-on-reflection* true)

(defn parse-opts
  "Extract the opts from the transaction and keywordify the top level keys."
  [expanded-txn]
  (let [string-opts (get-first-value expanded-txn const/iri-opts)]
    (reduce-kv (fn [opts k v]
                 (assoc opts (keyword k) v))
               {} string-opts)))

(defn create-ledger!
  [conn broadcaster watcher {:keys [ledger-id txn opts] :as params}]
  (with-log-attributes {:current (get-trace-id (otel-context/current))
                        :dyn (get-trace-id (otel-context/dyn))}
    (log/debug "create ledger"))
  (go-try
    (let [create-opts   (parse-opts txn)
          ledger        (deref!
                         (fluree/create conn ledger-id create-opts))
          staged-db     (-> ledger
                            fluree/db
                            (fluree/stage txn opts)
                            deref!)
          result        (deref!
                         (fluree/apply-stage! ledger staged-db))]
      (broadcast/broadcast-new-ledger! broadcaster watcher params result))))

(defn transact!
  [conn broadcaster watcher {:keys [ledger-id tx-id txn opts] :as params}]
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
          result        (deref!
                         (fluree/apply-stage! ledger staged-db))]
      (broadcast/broadcast-new-commit! broadcaster watcher params result))))

(defn process-event
  [conn broadcaster watcher event]
  (let [ctx (-> event meta :otel-context)]
    (with-log-attributes {:current (get-trace-id (otel-context/current))
                          :dyn (get-trace-id (otel-context/dyn))
                          :ctx (get-trace-id ctx)}
      (log/debug "process before with-context"))
    (otel-context/with-context! ctx
      (with-log-attributes {:current (get-trace-id (otel-context/current))
                            :dyn (get-trace-id (otel-context/dyn))}
        (log/debug "process before go"))
      (go
        (with-log-attributes {:current (get-trace-id (otel-context/current))
                              :dyn (get-trace-id (otel-context/dyn))}
          (log/debug "process start go"))

        (try
          (otel-context/with-bound-context!
            (with-log-attributes {:current (get-trace-id (otel-context/current))
                                  :dyn (get-trace-id (otel-context/dyn))}
              (log/debug "with bound context "))
            (let [event-type (events/event-type event)
                  result     (<! (case event-type
                                   :ledger-create (create-ledger! conn broadcaster watcher event)
                                   :tx-queue      (transact! conn broadcaster watcher event)))]
              (if (exception? result)
                (broadcast/broadcast-error! broadcaster watcher event result)
                result)))
          (catch Exception e
            (log/error e
                       "Unexpected event message - expected a map with a supported "
                       "event type. Received:" event)))))))

(defn new-transaction-queue
  ([conn broadcaster watcher]
   (new-transaction-queue conn broadcaster watcher nil))
  ([conn broadcaster watcher max-pending-txns]
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
                 result         (<! (process-event conn broadcaster watcher event))
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
  [conn broadcaster watcher max-pending-txns]
  (let [tx-queue (new-transaction-queue conn broadcaster watcher max-pending-txns)]
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
  [conn broadcaster watcher]
  (let [tx-queue (new-transaction-queue conn broadcaster watcher)]
    (->SynchronizedTransactor tx-queue)))

(defn start
  ([conn broadcaster watcher]
   (start-synchronized conn broadcaster watcher))
  ([conn broadcaster watcher max-pending-txns]
   (if max-pending-txns
     (start-buffered conn broadcaster watcher max-pending-txns)
     (start-synchronized conn broadcaster watcher))))

(defn stop
  [{:keys [tx-queue] :as _transactor}]
  (async/close! tx-queue))
