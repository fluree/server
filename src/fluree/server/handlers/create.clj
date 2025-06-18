(ns fluree.server.handlers.create
  (:require [fluree.db.api :as fluree]
            [fluree.db.util.log :as log]
            [fluree.server.consensus :as consensus]
            [fluree.server.handlers.shared :as shared :refer [deref! defhandler]]
            [fluree.server.handlers.transact :as srv-tx]
            [fluree.server.watcher :as watcher]))

(set! *warn-on-reflection* true)

(defn queue-consensus
  [consensus watcher ledger-id tx-id txn opts]
  (let [;; initial response is not completion, but acknowledgement of persistence
        persist-resp-ch (consensus/queue-new-ledger consensus ledger-id tx-id txn opts)]
    (consensus/monitor-consensus watcher ledger-id persist-resp-ch :tx-id tx-id)))

(defn create-ledger!
  [consensus watcher ledger-id txn opts]
  (let [p         (promise)
        tx-id     (srv-tx/derive-tx-id txn)
        result-ch (watcher/create-watch watcher tx-id)]
    (queue-consensus consensus watcher ledger-id tx-id txn opts)
    (srv-tx/monitor-commit p ledger-id tx-id result-ch)
    p))

(defhandler default
  [{:keys          [fluree/opts fluree/consensus fluree/watcher]
    {:keys [body]} :parameters}]
  (log/debug "create body:" body)
  (let [txn          (fluree/format-txn body opts)
        ledger-id    (or (:ledger opts)
                         (srv-tx/extract-ledger-id txn))
        opts*        (dissoc opts :identity) ; Remove identity option because the
                                             ; request should have been validated
                                             ; upstream, and there are no policies
                                             ; in an empty ledger to allow any
                                             ; actions
        commit-event (deref! (create-ledger! consensus watcher ledger-id txn opts*))

        body (srv-tx/commit-event->response-body commit-event)]
    (shared/with-tracking-headers {:status 201, :body body}
      commit-event)))
