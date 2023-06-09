(ns fluree.server.consensus.handlers.tx-queue
  (:require [clojure.core.async :as async]
            [fluree.db.json-ld.api :as fluree]
            [fluree.db.util.json :as json]
            [fluree.db.util.log :as log]
            [fluree.http-api.handlers.ledger :refer [deref!]]
            [fluree.server.consensus.producers.new-commit :refer [consensus-push-commit]]
            [fluree.server.consensus.producers.new-index-file :refer [push-new-index-files]]))

(set! *warn-on-reflection* true)

(comment
  ;; Operations below use a tx-queue parameter map which looks like as follows.
  ;; This map is created in the 'transact' http handler via
  ;; fluree.server.consensus.core/queue-new-transaction

  {:txn       txn
   :conn-type conn-type
   :size      (count txn)
   :tx-id     tx-id
   :ledger-id ledger-id
   :instant   (System/currentTimeMillis)}

  )

;; holds a 'lock' per ledger while processing a transaction
(def tx-processing-lock (atom {}))

(defn- acquire-lock
  "Tries to get a lock for ledger to process a transaction.
  Returns true if successful.

  If a different transaction is currently being processed returns false."
  [ledger-id tx-id]
  (let [new-state (swap! tx-processing-lock
                         (fn [current-state]
                           (if (contains? current-state ledger-id)
                             current-state
                             (assoc current-state ledger-id {:tx-id      tx-id
                                                             :start-time (System/currentTimeMillis)}))))
        success?  (= tx-id (get-in new-state [ledger-id :tx-id]))]
    success?))


(defn- update-lock
  "If after a transaction is processed there is another transaction
  in the queue, updates the lock for the new transaction."
  [ledger-id tx-id]
  (swap! tx-processing-lock assoc ledger-id {:tx-id      tx-id
                                             :start-time (System/currentTimeMillis)}))


(defn- release-lock
  "Releases lock for a ledger."
  [ledger-id]
  (swap! tx-processing-lock dissoc ledger-id))


(defn my-responsibility?
  "Returns true if this server is currently responsible for processing
  transactions for this particular ledger.

  For now, only the leader is responsible."
  [raft-state ledger-id]
  (= :leader (:status raft-state)))


(defn txn-body->opts
  [{:keys [context txn] :as _body}]
  (let [first-txn (if (map? txn)
                    txn
                    (first txn))]
    (cond-> {}
            (-> first-txn keys first keyword?) (assoc :context-type :keyword)
            (-> first-txn keys first string?) (assoc :context-type :string)
            context (assoc :context context))))


(defn do-transaction
  [{:keys [:fluree/conn] :as config} {:keys [ledger-id tx-id txn] :as params}]
  (let [start-time (System/currentTimeMillis)
        _          (log/debug "Starting transaction processing for ledger:" ledger-id
                              "with tx-id" tx-id ". Transaction sat in queue for"
                              (- start-time (:instant params)) "milliseconds.")
        ledger     (if (deref! (fluree/exists? conn ledger-id))
                     (deref! (fluree/load conn ledger-id))
                     (throw (ex-info "Ledger does not exist" {:ledger ledger-id})))
        txn'       (json/parse txn false)
        opts       (txn-body->opts {:txn txn'})
        commit!    (fn [db]
                     (let [index-files-ch (async/chan)
                           _              (push-new-index-files config index-files-ch) ;; monitor for new index files and push across network
                           resp           (fluree/commit! ledger db {:file-data?     true
                                                                     :index-files-ch index-files-ch})]
                       (log/debug "New commit for ledger" ledger-id "with tx-id: " tx-id
                                  "processed in" (- (System/currentTimeMillis) start-time) "milliseconds.")
                       resp))]
    (-> ledger
        fluree/db
        (fluree/stage txn' opts)
        deref!
        (commit!))))


(defn get-next-transaction
  "Checks the consensus state machine to see if any more transactions
  are queued for the ledger. If so, return it and updates the lock
  to reflect the new transaction being worked on."
  [{:keys [:consensus/state-atom :consensus/raft-state] :as config} processed-tx-ids ledger-id]
  (let [queue             (vals (get-in @state-atom [:tx-queue ledger-id]))
        processed-tx-ids' (set processed-tx-ids)]
    (->> queue
         (sort-by :instant) ;; earliest in queue first
         (some #(when-not (processed-tx-ids' (:tx-id %)) ;; return first not in processed-tx-ids
                  %)))))


(defn- trim-processed
  "If a ledger is under constant transactional load there will
  always be transactions queued waiting for processing. Since we keep track
  of ones that have been processed since there has last been none waiting in
  the queue, a ledger that never has a break would grow the list indefinitely.

  This function sets an upper limit to the list growth, and trims it if
  necessary. This would only be required under that 'constant load'
  scenario, but in the unlikely case it exists this prevents unbound
  growth of that list."
  [processed]
  (let [max 100]
    (if (> (count processed) max)
      (take (quot max 2) processed)
      processed)))


(defn process-transactions
  "Processes transaction and pushes result through consensus.

  After completion, checks if any new transactions have been
  queued while processing, and if so processes them. Continues
  until there are no queued transactions."
  [config next-txn-map]
  (loop [{:keys [ledger-id tx-id] :as next-txn-map} next-txn-map
         processed (list)] ;; list of transactions that have been evaluated
    ;; TODO - push-consensus will only work if a leader. As leader can only do txns for the moment this is OK
    ;; TODO - but once the work gets spread out to other servers it will have to push the commit to the leader
    (let [tx-result      (deref! (do-transaction config next-txn-map))
          consensus-push (consensus-push-commit config next-txn-map tx-result)
          processed*     (conj processed tx-id)]
      (if-let [next-txn-in-queue (get-next-transaction config processed* ledger-id)]
        (do
          (update-lock ledger-id (:tx-id next-txn-in-queue))
          (recur next-txn-in-queue (trim-processed processed*)))
        ;; if no more transactions, return last consensus push
        (do
          (release-lock ledger-id)
          consensus-push)))))


(defn processor
  "Processes a new transaction request.

  Only the leader creates new ledgers."
  [{:keys [:consensus/raft-state] :as config} {:keys [ledger-id tx-id] :as params}]
  (when (and (my-responsibility? raft-state ledger-id)
             (acquire-lock config ledger-id))
    (process-transactions config params)))


(defn handler
  "Stores a new transaction into the queue. Exerts backpressure if too many transactions
  are already queued."
  [{:keys [:consensus/state-atom :consensus/raft-state] :as config} {:keys [ledger-id tx-id] :as params}]
  (log/debug "Queuing new transaction into state machine with params: " params)
  (try
    (let [max-queue 100 ;; TODO - elevate this to a configuration option
          queued-n  (count (get-in @state-atom [:tx-queue ledger-id]))]
      (if (< queued-n max-queue)
        (do
          (swap! state-atom assoc-in [:tx-queue ledger-id tx-id] params)
          ;; return original parameters for the next step (processing) as applicable
          params)
        (throw (ex-info (str "Maximum number of transactions (" max-queue
                             ") are queued for ledger: " ledger-id ".")
                        {:status 429 ;; too many requests code / backpressure
                         :error  :db/queue-full}))))
    (catch Exception e
      ; return exception (don't throw) for handler on error
      (log/warn (ex-message e))
      (if (ex-data e)
        e
        (ex-info (str "Unexpected error queuing new transaction: " (ex-message e))
                 {:status 500
                  :error  :db/unexpected-error}
                 e)))))