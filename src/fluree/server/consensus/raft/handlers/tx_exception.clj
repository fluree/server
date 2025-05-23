(ns fluree.server.consensus.raft.handlers.tx-exception
  (:require [fluree.db.util.log :as log]
            [fluree.server.consensus.response :as response]))

(defn update-ledger-state
  "Updates the latest commit in the ledger, and removes the processed transaction in the queue"
  [{:keys [:consensus/state-atom] :as _config}
   {:keys [ledger-id tx-id] :as exception-meta}]
  (try

    (swap! state-atom
           (fn [current-state]
             (-> current-state
                 ;; remove the transaction from the queue
                 (update-in [:tx-queue ledger-id] dissoc tx-id))))
    exception-meta

    (catch Exception e
      ; return exception (don't throw) for handler on error
      (log/warn (ex-message e))
      (if (ex-data e)
        e
        (ex-info (str "Unexpected error removing tx-id from tx-queue after exception. Ledger: " ledger-id
                      " tx-id: " tx-id " message: " (ex-message e))
                 {:status 500
                  :error  :db/unexpected-error}
                 e)))))

(defn deliver!
  [{:keys [fluree/watcher fluree/broadcaster] :as _config} exception]
  (response/announce-error watcher broadcaster nil nil exception))

(defn handler
  "Handles transaction exceptions and broadcasts them to network."
  [config exception-meta]
  (log/debug (str "Transaction exception received via consensus: " exception-meta))
  (try

    (->> exception-meta
         (update-ledger-state config)
         (deliver! config))

    (catch Exception e
      (log/warn (str "Error recording transaction exception: " (ex-message e)))
      (if (ex-data e)
        e
        (ex-info (str "Unexpected error recording transaction exception: " (ex-message e))
                 {:status 500
                  :error  :db/unexpected-error}
                 e)))))
