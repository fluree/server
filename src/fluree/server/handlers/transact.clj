(ns fluree.server.handlers.transact
  (:refer-clojure :exclude [update])
  (:require [clojure.set :refer [rename-keys]]
            [fluree.crypto :as crypto]
            [fluree.db.api :as fluree]
            [fluree.db.query.fql.parse :as parse]
            [fluree.db.util.trace :as trace]
            [fluree.json-ld :as json-ld]
            [fluree.server.consensus :as consensus]
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
    (shared/monitor-consensus-persistence watcher ledger-id persist-resp-ch :tx-id tx-id)))

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

(defhandler update
  [{:keys          [fluree/consensus fluree/watcher credential/did fluree/opts raw-txn]
    {:keys [body]} :parameters}]
  (let [txn       (fluree/format-txn body opts)
        ledger-id (or (:ledger opts)
                      (extract-ledger-id txn))
        ;; Ensure the transaction includes the ledger for the underlying update! function
        txn-with-ledger (if (and (:ledger opts)
                                 (not (parse/get-named txn "ledger")))
                          (assoc txn "ledger" ledger-id)
                          txn)
        opts*     (cond-> (assoc opts :format :fql :op :update)
                    raw-txn (assoc :raw-txn raw-txn)
                    did     (assoc :did did))
        {:keys [status] :as commit-event}
        (trace/form ::update-handler {}
          (deref! (transact! consensus watcher ledger-id txn-with-ledger opts*)))

        body (commit-event->response-body commit-event)]
    (shared/with-tracking-headers {:status status, :body body}
      commit-event)))

(defhandler insert
  [{:keys [fluree/consensus fluree/watcher credential/did fluree/opts raw-txn]
    {insert-txn :body} :parameters}]
  (let [ledger-id (:ledger opts)
        opts*     (cond-> (assoc opts :op :insert)
                    raw-txn (assoc :raw-txn raw-txn)
                    did     (assoc :identity did))
        {:keys [status] :as commit-event}
        (trace/form ::insert-handler {}
          (deref! (transact! consensus watcher ledger-id insert-txn opts*)))

        body (commit-event->response-body commit-event)]
    (shared/with-tracking-headers {:status status, :body body}
      commit-event)))

(defhandler upsert
  [{:keys [fluree/consensus fluree/watcher credential/did fluree/opts raw-txn]
    {upsert-txn :body} :parameters}]
  (let [ledger-id (:ledger opts)
        opts*     (cond-> (assoc opts :op :upsert)
                    raw-txn (assoc :raw-txn raw-txn)
                    did     (assoc :identity did))
        {:keys [status] :as commit-event}
        (trace/form ::upsert-handler {}
          (deref! (transact! consensus watcher ledger-id upsert-txn opts*)))

        body (commit-event->response-body commit-event)]
    (shared/with-tracking-headers {:status status, :body body}
      commit-event)))
