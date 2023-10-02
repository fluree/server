(ns fluree.server.consensus.handlers.new-index
  (:require [fluree.db.util.log :as log]))

(set! *warn-on-reflection* true)

(defn handler
  "Registers a new index point in the state via an updated commit file."
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
