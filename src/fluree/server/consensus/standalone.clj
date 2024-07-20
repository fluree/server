(ns fluree.server.consensus.standalone
  (:require [clojure.core.async :as async :refer [<! >! go]]
            [fluree.db.api :as fluree]
            [fluree.db.constants :as const]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.core :refer [get-first-value]]
            [fluree.db.util.log :as log]
            [fluree.server.consensus :as consensus]
            [fluree.server.consensus.msg-format :as msg-format]
            [fluree.server.handlers.shared :refer [deref!]]
            [fluree.server.subscriptions :as subscriptions]
            [fluree.server.watcher :as watcher]))

(defrecord StandaloneTransactor [tx-queue]
  consensus/TxGroup
  (queue-new-ledger [_ ledger-id tx-id txn opts]
    (go
      (let [event-msg (msg-format/queue-new-ledger ledger-id tx-id txn opts)]
        (>! tx-queue event-msg)
        true)))

  (queue-new-transaction [_ ledger-id tx-id txn opts]
    (go
      (let [event-msg (msg-format/queue-new-transaction ledger-id tx-id txn opts)]
        (>! tx-queue event-msg)
        true))))

(defn broadcast-new-ledger!
  "Responsible for producing the event broadcast to connected peers."
  [subscriptions watcher {:keys [ledger-id server tx-id commit-file-meta] :as handler-result}]
  (log/info (str "New Ledger successfully created by server " server ": " ledger-id " with tx-id: " tx-id "."))
  (watcher/deliver-watch watcher tx-id handler-result)
  (subscriptions/send-message-to-all subscriptions "ledger-created" ledger-id (:json commit-file-meta))
  :success)

(defn parse-opts
  "Extract the opts from the transaction and keywordify the top level keys."
  [expanded-txn]
  (let [string-opts (get-first-value expanded-txn const/iri-opts)]
    (reduce-kv (fn [opts k v]
                 (assoc opts (keyword k) v))
               {} string-opts)))

(defn create-ledger!
  [conn subscriptions watcher {:keys [ledger-id txn opts tx-id] :as _params}]
  (go-try
    (let [create-opts    (parse-opts txn)
          ledger         (deref!
                          (fluree/create conn ledger-id create-opts))
          staged-db      (-> ledger
                             fluree/db
                             (fluree/stage txn opts)
                             deref!)
          commmit-result (deref!
                         ;; following uses :file-data? and will return map with {:keys [db data-file commit-file]}
                          (fluree/commit! ledger staged-db {:file-data? true}))]
      (broadcast-new-ledger! subscriptions watcher (assoc commmit-result :tx-id tx-id
                                                          :ledger-id ledger-id
                                                          :t 1)))))

(defn broadcast-new-commit!
  "Responsible for producing the event broadcast to connected peers."
  [subscriptions watcher {:keys [ledger-id tx-id server commit-file-meta] :as commit-result}]
  (log/info "New transaction completed for" ledger-id "tx-id: " tx-id "by server:" server)
  (watcher/deliver-watch watcher tx-id commit-result)
  (subscriptions/send-message-to-all subscriptions "new-commit" ledger-id (:json commit-file-meta))
  :success)

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
          broadcast-msg (msg-format/new-commit nil params commit-result)]
      (broadcast-new-commit! subscriptions watcher broadcast-msg))))

(defn process-event
  [conn subscriptions watcher event]
  (go
    (try
      (let [[event-type event-msg] event

            result (<? (case event-type
                         :ledger-create (create-ledger! conn subscriptions watcher event-msg)
                         :tx-queue      (transact! conn subscriptions watcher event-msg)))]
        result)
      (catch Exception e
        (log/error "Unexpected event message - expected two-tuple of [event-type event-data], "
                   "and of a supported event type. Received:" event e)))))

(defn new-tx-queue
  [conn subscriptions watcher]
  (let [tx-queue (async/chan 512)]
    (go
      (loop [i 0]
        (let [timeout-ch (async/timeout 5000)
              [event ch] (async/alts! [tx-queue timeout-ch])]
          (cond
            (= timeout-ch ch)
            (do
              (log/trace "No new transactions in 5 second."
                         "Processed" i "total transactions.")
              (recur i))

            (nil? event)
            (do
              (log/warn "Closing local transaction queue was closed. No new transactions will be processed.")
              ::closed)

            :else
            (let [result (<! (process-event conn subscriptions watcher event))
                  i*     (inc i)]
              (log/trace "Processed transaction #" i* ". Result:" result)
              (recur i*))))))
    tx-queue))

(defn start
  [conn subscriptions watcher]
  (let [tx-queue (new-tx-queue conn subscriptions watcher)]
    (->StandaloneTransactor tx-queue)))

(defn stop
  [{:keys [tx-queue] :as _transactor}]
  (async/close! tx-queue))
