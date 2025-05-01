(ns fluree.server.handlers.transact
  (:require [clojure.core.async :as async :refer [<! go]]
            [fluree.crypto :as crypto]
            [fluree.db.api :as fluree]
            [fluree.db.query.fql.parse :as parse]
            [fluree.db.util.core :as util]
            [fluree.db.util.log :as log]
            [fluree.json-ld :as json-ld]
            [fluree.server.consensus :as consensus]
            [fluree.server.consensus.events :as events]
            [fluree.server.handlers.shared :refer [defhandler deref!]]
            [fluree.server.watcher :as watcher]))

(set! *warn-on-reflection* true)

(defn derive-tx-id
  [txn]
  (if (string? txn)
    (crypto/sha2-256 txn :hex :string)
    (-> (json-ld/normalize-data txn)
        (crypto/sha2-256 :hex :string))))

(defn monitor-consensus-persistence
  [watcher ledger tx-id persist-resp-ch]
  (go
    (let [persist-resp (<! persist-resp-ch)]
      ;; check for exception trying to put txn in consensus, if so we must
      ;; deliver the watch here, but if successful the consensus process will
      ;; deliver the watch downstream
      (when (util/exception? persist-resp)
        (log/warn persist-resp "Error submitting transaction")
        (let [error-event (events/error ledger tx-id persist-resp)]
          (watcher/deliver-event watcher tx-id error-event))))))

(defn queue-consensus
  "Register a new commit with consensus"
  [consensus watcher ledger tx-id expanded-txn opts]
  (let [;; initial response is not completion, but acknowledgement of persistence
        persist-resp-ch (consensus/queue-new-transaction consensus ledger tx-id
                                                         expanded-txn opts)]
    (monitor-consensus-persistence watcher ledger tx-id persist-resp-ch)))

(defn monitor-commit
  "Wait for final commit result from consensus on `result-ch`, broadcast to any
  subscribers and deliver response to promise `out-p`"
  [out-p ledger-id tx-id result-ch]
  (go
    (let [result (async/<! result-ch)]
      (log/debug "HTTP API transaction final response: " result)
      (cond
        (= :timeout result)
        (let [ex (ex-info (str "Timeout waiting for transaction to complete for: "
                               ledger-id " with tx-id: " tx-id)
                          {:status 408
                           :error  :db/response-timeout
                           :ledger ledger-id
                           :tx-id  tx-id})]
          (deliver out-p ex))

        (nil? result)
        (let [ex (ex-info (str "Missing transaction result for ledger: "
                               ledger-id " with tx-id: " tx-id
                               ". Transaction may have processed, check ledger"
                               " for confirmation.")
                          {:status 500
                           :error  :db/response-missing
                           :ledger ledger-id
                           :tx-id  tx-id})]
          (deliver out-p ex))

        (util/exception? result)
        (deliver out-p result)

        (events/error? result)
        (let [ex (ex-info (:error-message result) (:error-data result))]
          (deliver out-p ex))

        :else
        (let [{:keys [ledger-id commit t tx-id]} result]
          (log/debug "Transaction completed for:" ledger-id "tx-id:" tx-id
                     "commit head:" commit)
          (deliver out-p {:ledger ledger-id
                          :commit commit
                          :t      t
                          :tx-id  tx-id}))))))

(defn transact!
  [consensus watcher ledger-id txn opts]
  (let [p         (promise)
        tx-id     (derive-tx-id txn)
        result-ch (watcher/create-watch watcher tx-id)]
    (queue-consensus consensus watcher ledger-id tx-id txn opts)
    (monitor-commit p ledger-id tx-id result-ch)
    p))

(defn extract-ledger-id
  "Extracts ledger-id from expanded json-ld transaction"
  [txn]
  (or (parse/get-named txn "ledger")
      (throw (ex-info "Invalid transaction, missing required key: ledger."
                      {:status 400, :error :db/invalid-transaction}))))

(defhandler default
  [{:keys          [fluree/consensus fluree/watcher credential/did fluree/opts raw-txn]
    {:keys [body]} :parameters}]
  (let [txn       (fluree/format-txn body opts)
        ledger-id (or (:ledger opts)
                      (extract-ledger-id txn))
        opts*     (cond-> (assoc opts :format :fql)
                    raw-txn (assoc :raw-txn raw-txn)
                    did     (assoc :did did))
        resp-p    (transact! consensus watcher ledger-id txn opts*)]
    {:status 200, :body (deref! resp-p)}))
