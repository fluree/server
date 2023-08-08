(ns fluree.server.handlers.transact
  (:require [clojure.core.async :as async]
            [fluree.crypto :as crypto]
            [fluree.db.conn.proto :as conn-proto]
            [fluree.db.json-ld.api :as fluree]
            [fluree.db.util.core :as util]
            [fluree.json-ld :as json-ld]
            [fluree.server.components.watcher :as watcher]
            [fluree.server.handlers.ledger :refer [error-catching-handler deref!]]
            [fluree.db.util.log :as log]
            [fluree.server.consensus.core :as consensus]))

(set! *warn-on-reflection* true)

(defn hash-txn
  "Hashes a string, JSON representation of a txn that should
  already be normalized."
  [json-txn]
  (crypto/sha2-256 json-txn :hex :string))

(defn normalize-txn
  [txn]
  (let [txn*  (json-ld/normalize-data
                txn
                {:algorithm :basic
                 :format    :application/json})
        tx-id (hash-txn txn*)]
    [tx-id txn*]))


(defn queue-consensus
  [consensus conn watcher ledger tx-id txn* defaultContext opts]
  (let [conn-type       (conn-proto/-method conn)
        ;; initial response is not completion, but acknowledgement of persistence
        persist-resp-ch (consensus/queue-new-transaction consensus conn-type ledger tx-id txn* defaultContext opts)]
    (async/go
      (let [persist-resp (async/<! persist-resp-ch)]
        ;; check for exception trying to put txn in consensus, if so we must deliver the
        ;; watch here, but if successful the consensus process will deliver the watch downstream
        (when (util/exception? persist-resp)
          (watcher/deliver-watch watcher tx-id persist-resp))))))


(defn transact!
  [p consensus conn watcher {:keys [ledger txn defaultContext opts]}]
  (let [[tx-id txn*]  (normalize-txn txn)
        final-resp-ch (watcher/create-watch watcher tx-id)]

    ;; register transaction into consensus
    (queue-consensus consensus conn watcher ledger tx-id txn* defaultContext opts)

    ;; wait for final response from consensus and deliver to promise
    (async/go
      (let [final-resp (async/<! final-resp-ch)]
        (log/debug "HTTP API transaction final response: " final-resp)
        (cond
          (= :timeout final-resp)
          (deliver p (ex-info
                       (str "Timeout waiting for transaction to complete for: " ledger " with tx-id: " tx-id)
                       {:status 408 :error :db/response-timeout}))

          (nil? final-resp)
          (deliver p (ex-info
                       (str "Unexpected close waiting for ledger transaction to complete for: " ledger
                            " with tx-id: " tx-id ". Transaction may have processed, check ledger for confirmation.")
                       {:status 500 :error :db/response-closed}))

          (util/exception? final-resp)
          (deliver p final-resp)

          :else
          (let [{:keys [ledger-id commit-file-meta t tx-id]} final-resp]
            (log/info "Transaction completed for:" ledger-id "tx-id:" tx-id
                      "commit head:" (:address commit-file-meta))
            (deliver p {:ledger ledger-id
                        :commit (:address commit-file-meta)
                        :t      (- t)
                        :tx-id  tx-id})))))))


(defn throw-ledger-doesnt-exist
  [ledger]
  (let [err-message (str "Ledger " ledger " does not exist!")]
    (throw (ex-info err-message
                    {:response {:status 409
                                :body   {:error err-message}}}))))

(def default
  (error-catching-handler
    (fn [{:keys [fluree/conn fluree/consensus fluree/watcher credential/did] {{:keys [ledger txn] :as body} :body} :parameters}]
      (log/debug "New transaction received for ledger:" ledger)
      (let [_exists? (or (deref! (fluree/exists? conn ledger))
                         (throw-ledger-doesnt-exist ledger))
            resp-p   (promise)]

        ;; kick of async process that will eventually deliver resp or exception to resp-p
        (transact! resp-p consensus conn watcher body)

        {:status 200
         :body   (deref! resp-p)}))))
