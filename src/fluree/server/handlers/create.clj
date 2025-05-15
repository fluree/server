(ns fluree.server.handlers.create
  (:require
   [fluree.db.api :as fluree]
   [fluree.db.util.log :as log]
   [fluree.server.consensus :as consensus]
   [fluree.server.handlers.shared :refer [deref! defhandler]]
   [fluree.server.handlers.transact :refer [derive-tx-id extract-ledger-id
                                            monitor-consensus-persistence
                                            monitor-commit]]
   [fluree.server.watcher :as watcher]))

(set! *warn-on-reflection* true)

(defn queue-consensus
  [consensus watcher ledger tx-id txn opts]
  (let [;; initial response is not completion, but acknowledgement of persistence
        persist-resp-ch (consensus/queue-new-ledger consensus ledger tx-id txn opts)]
    (monitor-consensus-persistence watcher ledger tx-id persist-resp-ch)))

(defn create-ledger
  [consensus watcher ledger-id txn opts]
  (let [p         (promise)
        tx-id     (derive-tx-id txn)
        result-ch (watcher/create-watch watcher tx-id)]
    (queue-consensus consensus watcher ledger-id tx-id txn opts)
    (monitor-commit p ledger-id tx-id result-ch)
    p))

(defhandler default
  [{:keys          [fluree/opts fluree/consensus fluree/watcher]
    {:keys [body]} :parameters}]
  (log/debug "create body:" body)
  (let [txn       (fluree/format-txn body opts)
        ledger-id (or (:ledger opts)
                      (extract-ledger-id txn))
        resp-p    (create-ledger consensus watcher ledger-id txn opts)]
    {:status 201, :body (deref! resp-p)}))
