(ns fluree.server.consensus.raft.handlers.ledger-created
  (:require [fluree.db.util.log :as log]
            [fluree.db.util.async :refer [<??]]
            [fluree.server.consensus.broadcast :as broadcast]
            [fluree.server.consensus.raft.handlers.new-commit :as new-commit]))

(set! *warn-on-reflection* true)

(defn verify-still-pending
  "Just received a new ledger creation commit, do a check to ensure the current raft
  state still has this request pending."
  [{:keys [consensus/state-atom] :as _config}
   {:keys [ledger-id] :as create-result}]
  (when-not (get-in @state-atom [:create-ledger ledger-id])
    (throw (ex-info (str "Cannot accept new ledger creation for ledger: " ledger-id
                         " as it is no longer queued for creation.")
                    {:status 500 :error :consensus/unexpected-error})))
  create-result)

(defn add-ledger-to-state
  [{:keys [consensus/state-atom] :as _config}
   {:keys [ledger-id commit-file-meta t] :as create-result}]
  (try
    (swap! state-atom
           (fn [current-state]
             (-> current-state
                 ;; remove the :create-ledger queued request
                 (update :create-ledger dissoc ledger-id)
                 ;; add in the new ledger to state
                 (assoc-in [:ledgers ledger-id] {:t              t
                                                 :status         :ready
                                                 :commit-address (:address commit-file-meta)}))))
    ;; return original params for broadcast step
    create-result

    (catch Exception e
      (let [ex (if (ex-data e)
                 e
                 (ex-info (str "Unexpected error queuing new ledger: " (ex-message e))
                          {:status 500
                           :error  :db/unexpected-error}
                          e))]
        (log/warn (ex-message ex))
        (throw ex)))))

(defn handler
  "Adds a new ledger along with its transaction into the state machine in a queue.
  The consensus leader is responsible for creating all new ledgers, and will get
  to it ASAP."
  [config create-result]
  (log/debug (str "New ledger created with params: " create-result))
  (try

    (->> create-result
         (verify-still-pending config)
         (new-commit/store-ledger-files config)
         <??
         (add-ledger-to-state config)
         (new-commit/push-nameservice config)
         <??)

    (catch Exception e
      (log/warn (str "Unexpected error creating new ledger: " (ex-message e)))
      (new-commit/delete-ledger-files config create-result)
      (let [e* (if (ex-data e)
                 e
                 (ex-info (str "Unexpected error creating new ledger: " (ex-message e))
                          {:status 500
                           :error  :db/unexpected-error}
                          e))]

        e*))))

(defn broadcast!
  "Responsible for producing the event broadcast to connected peers."
  [{:keys [fluree/watcher fluree/subscriptions] :as _config} handler-result]
  (broadcast/announce-new-ledger! subscriptions watcher handler-result))
