(ns fluree.server.consensus.handlers.ledger-created
  (:require [clojure.core.async :as async]
            [clojure.java.io :as io]
            [fluree.server.consensus.handlers.new-commit :as new-commit]
            [fluree.db.util.filesystem :as fs]
            [fluree.db.util.log :as log]
            [fluree.server.components.subscriptions :as subs]
            [fluree.server.components.watcher :as watcher])
  (:import (java.io File)))

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
      ; return exception (don't throw) for handler on error
      (log/warn (ex-message e))
      (if (ex-data e)
        e
        (ex-info (str "Unexpected error queuing new ledger: " (ex-message e))
                 {:status 500
                  :error  :db/unexpected-error}
                 e)))))


(defn return-success-response
  [watcher {:keys [ledger-id server tx-id] :as params} state-map]
  (log/info (str "New Ledger successfully created by server " server ": " ledger-id " with tx-id: " tx-id "."))
  (watcher/deliver-watch watcher tx-id params)
  (get-in state-map [:ledgers ledger-id]))


(defn clean-up-files
  [{:keys [fluree/conn] :as _config} {:keys [ledger-id] :as _params}]
  (let [local-path (fs/local-path (:storage-path conn))
        ledger-dir (io/file local-path ledger-id)
        files      (reverse (file-seq ledger-dir))]
    (doseq [^File file files]
      (log/info (str "After exception creating ledger " ledger-id ", removing: " (.getPath file)))
      (io/delete-file file true))))


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
         async/<!!
         (add-ledger-to-state config))

    (catch Exception e
      (log/warn (str "Unexpected error creating new ledger: " (ex-message e)))
      (clean-up-files config create-result)
      (let [e* (if (ex-data e)
                 e
                 (ex-info (str "Unexpected error creating new ledger: " (ex-message e))
                          {:status 500
                           :error  :db/unexpected-error}
                          e))]

        e*))))

(defn broadcast!
  "Responsible for producing the event broadcast to connected peers."
  [{:keys [fluree/watcher fluree/subscriptions] :as config}
   {:keys [ledger-id server tx-id commit-file-meta] :as handler-result}]
  (log/info (str "New Ledger successfully created by server " server ": " ledger-id " with tx-id: " tx-id "."))
  (watcher/deliver-watch watcher tx-id handler-result)
  (subs/send-message-to-all subscriptions "ledger-created" ledger-id (:json commit-file-meta))
  :success) ;; result of this function is not used