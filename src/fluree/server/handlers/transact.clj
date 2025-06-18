(ns fluree.server.handlers.transact
  (:require [clojure.core.async :as async :refer [<! go]]
            [clojure.set :refer [rename-keys]]
            [fluree.crypto :as crypto]
            [fluree.db.api :as fluree]
            [fluree.db.query.fql.parse :as parse]
            [fluree.db.util.core :as util]
            [fluree.db.util.log :as log]
            [fluree.json-ld :as json-ld]
            [fluree.server.consensus :as consensus]
            [fluree.server.consensus.events :as events]
            [fluree.server.handlers.shared :as shared :refer [defhandler deref!]]
            [fluree.server.watcher :as watcher]))

(set! *warn-on-reflection* true)

(defn derive-tx-id
  [txn]
  (if (string? txn)
    (crypto/sha2-256 txn :hex :string)
    (-> (json-ld/normalize-data txn)
        (crypto/sha2-256 :hex :string))))

(defn queue-consensus
  "Register a new commit with consensus"
  [consensus watcher ledger-id tx-id expanded-txn opts]
  (let [;; initial response is not completion, but acknowledgement of persistence
        persist-resp-ch (consensus/queue-new-transaction consensus ledger-id tx-id
                                                         expanded-txn opts)]
    (consensus/monitor-consensus watcher ledger-id persist-resp-ch :tx-id tx-id)))

(defn transact!
  [consensus watcher ledger-id txn opts]
  (let [p         (promise)
        tx-id     (derive-tx-id txn)
        result-ch (watcher/create-watch watcher tx-id)]
    (queue-consensus consensus watcher ledger-id tx-id txn opts)
    (watcher/monitor p ledger-id result-ch :tx-id tx-id)
    p))

(defn extract-ledger-id
  "Extracts ledger-id from expanded json-ld transaction"
  [txn]
  (or (parse/get-named txn "ledger")
      (throw (ex-info "Invalid transaction, missing required key: ledger."
                      {:status 400, :error :db/invalid-transaction}))))

(defn commit-event->response-body
  [commit-event]
  (-> commit-event
      (select-keys [:ledger-id :commit :t :tx-id])
      (rename-keys {:ledger-id :ledger})))

(defhandler default
  [{:keys          [fluree/consensus fluree/watcher credential/did fluree/opts raw-txn]
    {:keys [body]} :parameters}]
  (let [txn       (fluree/format-txn body opts)
        ledger-id (or (:ledger opts)
                      (extract-ledger-id txn))
        opts*     (cond-> (assoc opts :format :fql)
                    raw-txn (assoc :raw-txn raw-txn)
                    did     (assoc :did did))
        {:keys [status] :as commit-event}
        (deref! (transact! consensus watcher ledger-id txn opts*))

        body (commit-event->response-body commit-event)]
    (shared/with-tracking-headers {:status status, :body body}
      commit-event)))
