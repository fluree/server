(ns fluree.server.handlers.transact
  (:require [clojure.core.async :as async]
            [fluree.crypto :as crypto]
            [fluree.db.api :as fluree]
            [fluree.db.api.transact :as transact-api]
            [fluree.db.constants :as const]
            [fluree.db.util.context :as ctx-util]
            [fluree.db.util.core :as util :refer [get-first-value]]
            [fluree.db.util.log :as log]
            [fluree.json-ld :as json-ld]
            [fluree.server.consensus :as consensus]
            [fluree.server.consensus.watcher :as watcher]
            [fluree.server.handlers.shared :refer [defhandler deref!]]))

(set! *warn-on-reflection* true)

(defn derive-tx-id
  [raw-txn]
  (if (string? raw-txn)
    (crypto/sha2-256 raw-txn :hex :string)
    (-> (json-ld/normalize-data raw-txn)
        (crypto/sha2-256 :hex :string))))

(defn queue-consensus
  [consensus watcher ledger tx-id expanded-txn opts]
  (let [;; initial response is not completion, but acknowledgement of persistence
        persist-resp-ch (consensus/queue-new-transaction consensus ledger tx-id
                                                         expanded-txn opts)]
    (async/go
      (let [persist-resp (async/<! persist-resp-ch)]
        ;; check for exception trying to put txn in consensus, if so we must deliver the
        ;; watch here, but if successful the consensus process will deliver the watch downstream
        (when (util/exception? persist-resp)
          (watcher/deliver-error watcher tx-id persist-resp))))))

(defn transact!
  [consensus watcher txn opts]
  (let [p             (promise)
        ledger-id     (transact-api/extract-ledger-id txn)
        tx-id         (derive-tx-id (:raw-txn opts))
        final-resp-ch (watcher/create-watch watcher tx-id)]

    ;; register transaction into consensus
    (queue-consensus consensus watcher ledger-id tx-id txn opts)

    ;; wait for final response from consensus and deliver to promise
    (async/go
      (let [final-resp (async/<! final-resp-ch)]
        (log/debug "HTTP API transaction final response: " final-resp)
        (cond
          (= :timeout final-resp)
          (deliver p (ex-info
                      (str "Timeout waiting for transaction to complete for: "
                           ledger-id " with tx-id: " tx-id)
                      {:status 408 :error :db/response-timeout}))

          (nil? final-resp)
          (deliver p (ex-info
                      (str "Unexpected close waiting for ledger transaction to complete for: "
                           ledger-id " with tx-id: " tx-id
                           ". Transaction may have processed, check ledger for confirmation.")
                      {:status 500 :error :db/response-closed}))

          (util/exception? final-resp)
          (deliver p final-resp)

          :else
          (let [{:keys [ledger-id commit t tx-id]} final-resp]
            (log/info "Transaction completed for:" ledger-id "tx-id:" tx-id
                      "commit head:" commit)
            (deliver p {:ledger ledger-id
                        :commit commit
                        :t      t
                        :tx-id  tx-id})))))
    p))

(defn throw-ledger-doesnt-exist
  [ledger]
  (let [err-message (str "Ledger " ledger " does not exist!")]
    (throw (ex-info err-message
                    {:response {:status 409
                                :body   {:error err-message}}}))))

(defhandler default
  [{:keys [fluree/conn fluree/consensus fluree/watcher credential/did raw-txn]
    {:keys [body]} :parameters}]
  (let [txn-context    (ctx-util/txn-context body)
        ledger-id      (transact-api/extract-ledger-id body)]
    (if (deref! (fluree/exists? conn ledger-id))
      (let [opts   (cond-> {:context txn-context :raw-txn raw-txn}
                     did (assoc :did did))
            resp-p (transact! consensus watcher body opts)]
        {:status 200, :body (deref! resp-p)})
      (throw-ledger-doesnt-exist ledger-id))))

(defhandler callback
  [{:fluree/keys   [watcher]
    {:keys [body]} :parameters}]
  (let [{:keys [tx-id status]} body]
    (if (= status "ok")
      (let [event (dissoc body :status)]
        (watcher/deliver-commit watcher tx-id event))
      (let [{:keys [error-msg error-data]}
            body
            ex (ex-info error-msg error-data)]
        (watcher/deliver-error watcher tx-id ex)))
    {:status 200, :body {:status "ok"}}))
