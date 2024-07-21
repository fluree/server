(ns fluree.server.consensus.standalone
  (:require [clojure.core.async :as async :refer [<! go go-loop]]
            [fluree.db.api :as fluree]
            [fluree.db.constants :as const]
            [fluree.db.util.async :refer [go-try]]
            [fluree.db.util.core :refer [exception? get-first-value]]
            [fluree.db.util.log :as log]
            [fluree.server.consensus :as consensus]
            [fluree.server.consensus.broadcast :as broadcast]
            [fluree.server.consensus.events :as events]
            [fluree.server.handlers.shared :refer [deref!]]))

(defrecord StandaloneTransactor [tx-queue]
  consensus/Transactor
  (-queue-new-ledger [_ ledgerevent]
    (go (async/offer! tx-queue ledgerevent)))

  (-queue-new-transaction [_ txn-evt]
    (go (async/offer! tx-queue txn-evt))))

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
          broadcast-evt (events/transaction-committed params commit-result)]
      (broadcast/announce-new-ledger! subscriptions watcher broadcast-evt))))

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
          broadcast-evt (events/transaction-committed params commit-result)]
      (broadcast/announce-new-commit! subscriptions watcher broadcast-evt))))

(defn process-event
  [conn subscriptions watcher event]
  (go
    (try
      (let [[event-type event-evt] event

            result (<! (case event-type
                         :ledger-create (create-ledger! conn subscriptions watcher event-evt)
                         :tx-queue      (transact! conn subscriptions watcher event-evt)))]
        (if (exception? result)
          (let [error-evt (events/error event-evt result)]
            (broadcast/announce-error! watcher error-evt))
          result))
      (catch Exception e
        (log/error "Unexpected event message - expected two-tuple of [event-type event-data], "
                   "and of a supported event type. Received:" event e)))))

(defn new-tx-queue
  [conn subscriptions watcher]
  (let [tx-queue (async/chan 16)]
    (go-loop [i 0]
      (let [timeout-ch (async/timeout 5000)
            [event ch] (async/alts! [tx-queue timeout-ch])]
        (cond
          (= timeout-ch ch)
          (do
            (log/trace "No new transactions in 5 seconds."
                       "Processed" i "total transactions.")
            (recur i))

          (nil? event)
          (do
            (log/warn "Local transaction queue was closed. No new transactions will be processed.")
            ::closed)

          :else
          (let [result (<! (process-event conn subscriptions watcher event))
                i*     (inc i)]
            (log/trace "Processed transaction #" i* ". Result:" result)
            (recur i*)))))
    tx-queue))

(defn start
  [conn subscriptions watcher]
  (let [tx-queue (new-tx-queue conn subscriptions watcher)]
    (->StandaloneTransactor tx-queue)))

(defn stop
  [{:keys [tx-queue] :as _transactor}]
  (async/close! tx-queue))
