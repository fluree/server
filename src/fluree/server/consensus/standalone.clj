(ns fluree.server.consensus.standalone
  (:require [clojure.core.async :as async :refer [<! >! go go-loop]]
            [fluree.db.api :as fluree]
            [fluree.db.constants :as const]
            [fluree.db.util.async :refer [go-try]]
            [fluree.db.util.core :refer [exception? get-first-value]]
            [fluree.db.util.log :as log]
            [fluree.server.consensus :as consensus]
            [fluree.server.consensus.events :as events]
            [fluree.server.handlers.shared :refer [deref!]]
            [fluree.server.watcher :as watcher]
            [steffan-westcott.clj-otel.api.trace.span :as span]
            [steffan-westcott.clj-otel.context :as otel-context]))

(set! *warn-on-reflection* true)

(defn parse-opts
  "Extract the opts from the transaction and keywordify the top level keys."
  [expanded-txn]
  (let [string-opts (get-first-value expanded-txn const/iri-opts)]
    (reduce-kv (fn [opts k v]
                 (assoc opts (keyword k) v))
               {} string-opts)))

(defn create-ledger!
  [conn watcher {:keys [ledger-id tx-id txn opts] :as _params}]
  (go-try
    (let [create-opts   (parse-opts txn)
          ledger        (deref!
                         (fluree/create conn ledger-id create-opts))
          staged-db     (-> ledger
                            fluree/db
                            (fluree/stage txn opts)
                            deref!)
          commit-result (deref! (fluree/apply-stage! ledger staged-db))]
      (watcher/deliver-new-ledger watcher ledger-id tx-id commit-result))))

(defn transact!
  [conn watcher {:keys [ledger-id tx-id txn opts] :as params}]
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
          commit-result (deref! (fluree/apply-stage! ledger staged-db))]
      (watcher/deliver-commit watcher ledger-id tx-id commit-result))))

(defn process-event
  [conn watcher event]
  (span/with-span!  {:name "fluree.server.consensus.standalone/process-event"
                    ;; should be :consumer but only :server supported by xray  https://github.com/aws-observability/aws-otel-collector/issues/1773
                     :span-kind :server
                     :parent (consensus/get-trace-context event)}
    (go
      (try
        (let [event-type (events/event-type event)
              result     (<! (case event-type
                               :ledger-create (create-ledger! conn watcher event)
                               :tx-queue      (transact! conn watcher event)))]
          (if (exception? result)
            (let [{:keys [ledger-id tx-id]} event]
              (log/debug result "Delivering tx-exception to watcher")
              (watcher/deliver-error watcher ledger-id tx-id result))
            result))
        (catch Exception e
          (log/error e
                     "Unexpected event message - expected a map with a supported "
                     "event type. Received:" event)
          ::error)))))

(defn error?
  [result]
  (or (= result ::error)
      (nil? result)))

(defn new-transaction-queue
  ([conn watcher]
   (new-transaction-queue conn watcher nil))
  ([conn watcher max-pending-txns]
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
                 result         (<! (process-event conn watcher event))
                 i*             (inc i)]
             (log/trace "Processed transaction #" i* ". Result:" result)
             (when-not (error? result)
               (async/put! out-ch result (fn [closed?]
                                           (when-not closed?
                                             (async/close! out-ch)))))
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
  [conn watcher max-pending-txns]
  (let [tx-queue (new-transaction-queue conn watcher max-pending-txns)]
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
  [conn watcher]
  (let [tx-queue (new-transaction-queue conn watcher)]
    (->SynchronizedTransactor watcher tx-queue)))

(defn start
  ([conn watcher]
   (start-synchronized conn watcher))
  ([conn watcher max-pending-txns]
   (if max-pending-txns
     (start-buffered conn watcher max-pending-txns)
     (start-synchronized conn watcher))))

(defn stop
  [{:keys [tx-queue] :as _transactor}]
  (async/close! tx-queue))
