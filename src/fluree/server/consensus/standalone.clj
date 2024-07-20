(ns fluree.server.consensus.standalone
  (:require [clojure.core.async :as async]
            [fluree.db.constants :as const]
            [fluree.db.api :as fluree]
            [fluree.db.util.async :refer [go-try]]
            [fluree.db.util.log :as log]
            [fluree.server.components.subscriptions :as subs]
            [fluree.server.consensus :as consensus]
            [fluree.server.consensus.msg-format :as msg-format]
            [fluree.server.consensus.watcher :as watcher]
            [fluree.server.handlers.shared :refer [deref!]]))

(defn queue-new-ledger
  [tx-queue ledger-id tx-id txn opts]
  (let [event-msg (msg-format/queue-new-ledger ledger-id tx-id txn opts)
        success?  (async/put! tx-queue event-msg)]
    (async/go success?)))

(defn queue-new-transaction
  [tx-queue ledger-id tx-id txn opts]
  (let [event-msg (msg-format/queue-new-transaction ledger-id tx-id txn opts)
        success?  (async/put! tx-queue event-msg)]
    (async/go success?)))

(defrecord StandaloneTransactor [tx-queue]
  consensus/TxGroup
  (queue-new-ledger [_ ledger-id tx-id txn opts]
    (queue-new-ledger tx-queue ledger-id tx-id txn opts))
  (queue-new-transaction [_ ledger-id tx-id txn opts]
    (queue-new-transaction tx-queue ledger-id tx-id txn opts)))

(defn broadcast-new-ledger!
  "Responsible for producing the event broadcast to connected peers."
  [{:keys [fluree/watcher fluree/subscriptions] :as _config}
   {:keys [ledger-id server tx-id commit-file-meta] :as handler-result}]
  (log/info (str "New Ledger successfully created by server " server ": " ledger-id " with tx-id: " tx-id "."))
  (watcher/deliver-watch watcher tx-id handler-result)
  (subs/send-message-to-all subscriptions "ledger-created" ledger-id (:json commit-file-meta))
  :success)

(defn parse-opts
  "Extract the opts from the transaction and keywordify the top level keys."
  [expanded-txn]
  (-> expanded-txn
      (get const/iri-opts)
      (get 0)
      (get "@value")
      (->> (reduce-kv (fn [opts k v] (assoc opts (keyword k) v)) {}))))

(defn do-new-ledger
  [{:keys [fluree/conn] :as config}
   {:keys [ledger-id txn opts tx-id] :as _params}]
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
      (broadcast-new-ledger! config (assoc commmit-result :tx-id tx-id
                                           :ledger-id ledger-id
                                           :t 1)))))

(defn broadcast-new-commit!
  "Responsible for producing the event broadcast to connected peers."
  [{:keys [fluree/watcher fluree/subscriptions] :as _config}
   {:keys [ledger-id tx-id server commit-file-meta] :as commit-result}]
  (log/info "New transaction completed for" ledger-id "tx-id: " tx-id "by server:" server)
  (watcher/deliver-watch watcher tx-id commit-result)
  (subs/send-message-to-all subscriptions "new-commit" ledger-id (:json commit-file-meta))
  :success)

(defn do-transaction
  [{:keys [fluree/conn] :as config}
   {:keys [ledger-id tx-id txn opts] :as params}]
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
      (broadcast-new-commit! config broadcast-msg))))

(defn process-event
  [config event]
  (async/go
    (try
      (let [[event-type event-msg] event
            result (async/<!
                    (case event-type
                      :ledger-create (do-new-ledger config event-msg)
                      :tx-queue (do-transaction config event-msg)))]
        result)
      (catch Exception e
        (log/error "Unexpected event message - expected two-tuple of [event-type event-data], "
                   "and of a supported event type. Received:" event e)))))

(defn monitor-new-tx-queue
  [config tx-queue]
  (async/go
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
          (let [result (async/<! (process-event config event))
                i*     (inc i)]
            (log/trace "Processed transaction #" i* ". Result:" result)
            (recur i*)))))))

(defn start
  [config]
  (let [tx-queue (async/chan)]
    (monitor-new-tx-queue config tx-queue)
    (->StandaloneTransactor tx-queue)))

(defn stop
  [{:keys [tx-queue] :as _transactor}]
  (async/close! tx-queue))
