(ns fluree.server.handlers.create
  (:require [fluree.db.api :as fluree]
            [fluree.server.consensus :as consensus]
            [fluree.server.handlers.shared :as shared :refer [deref! defhandler]]
            [fluree.server.handlers.transact :as srv-tx]
            [fluree.server.watcher :as watcher]))

(set! *warn-on-reflection* true)

(defn queue-consensus
  [consensus watcher ledger-id tx-id txn opts]
  (let [;; initial response is not completion, but acknowledgement of persistence
        persist-resp-ch (consensus/queue-new-ledger consensus ledger-id tx-id txn opts)]
    (shared/monitor-consensus-persistence watcher ledger-id persist-resp-ch :tx-id tx-id)))

(defn create-ledger!
  [consensus watcher ledger-id txn opts]
  (let [p         (promise)
        tx-id     (srv-tx/derive-tx-id txn)
        result-ch (watcher/create-watch watcher tx-id)]
    (queue-consensus consensus watcher ledger-id tx-id txn opts)
    (watcher/monitor p ledger-id result-ch :tx-id tx-id)
    p))

(defn- extract-ledger-id
  "Extracts ledger ID from request body, options, or transaction."
  [body opts txn]
  (or (get body "ledger")
      (:ledger opts)
      (when txn (srv-tx/extract-ledger-id txn))))

(defn- has-txn-data?
  "Checks if request body contains transaction data."
  [body]
  (contains? body "insert"))

(defn- prepare-create-options
  "Prepares options for ledger creation.
  Removes :identity since empty ledgers have no policies to check."
  [opts]
  (dissoc opts :identity))

(defhandler default
  [{:keys          [fluree/opts fluree/consensus fluree/watcher]
    {:keys [body]} :parameters}]
  (let [txn           (when (has-txn-data? body)
                        (fluree/format-txn body opts))
        ledger-id     (extract-ledger-id body opts txn)
        _             (when-not ledger-id
                        (throw (ex-info "Ledger ID must be provided"
                                        {:status 400 :error :db/invalid-ledger-id})))
        opts*         (prepare-create-options opts)
        commit-event  (deref! (create-ledger! consensus watcher ledger-id txn opts*))
        response-body (srv-tx/commit-event->response-body commit-event)]
    (shared/with-tracking-headers {:status 201, :body response-body}
      commit-event)))
