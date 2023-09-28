(ns fluree.server.handlers.transact
  (:require [clojure.core.async :as async]
            [fluree.crypto :as crypto]
            [fluree.db.api.transact :as transact-api]
            [fluree.db.conn.proto :as conn-proto]
            [fluree.db.constants :as const]
            [fluree.db.json-ld.api :as fluree]
            [fluree.db.util.core :as util]
            [fluree.json-ld :as json-ld]
            [fluree.server.components.watcher :as watcher]
            [fluree.server.handlers.shared :refer [defhandler deref!]]
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
  [consensus conn watcher ledger tx-id txn* default-context opts]
  (let [conn-type       (conn-proto/-method conn)
        ;; initial response is not completion, but acknowledgement of persistence
        persist-resp-ch (consensus/queue-new-transaction
                         consensus conn-type ledger tx-id txn* default-context
                         opts)]
    (async/go
      (let [persist-resp (async/<! persist-resp-ch)]
        ;; check for exception trying to put txn in consensus, if so we must deliver the
        ;; watch here, but if successful the consensus process will deliver the watch downstream
        (when (util/exception? persist-resp)
          (watcher/deliver-watch watcher tx-id persist-resp))))))

(defn transact!
  [p consensus conn watcher parsed-txn]
  (let [{ledger-id       const/iri-ledger
         txn             "@graph"
         {:keys [default-context] :as opts} :opts} parsed-txn
        [tx-id txn*] (normalize-txn txn)
        final-resp-ch (watcher/create-watch watcher tx-id)]

    ;; register transaction into consensus
    (queue-consensus consensus conn watcher ledger-id tx-id txn*
                     default-context opts)

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

(defhandler default
  [{:keys [fluree/conn fluree/consensus fluree/watcher]
    {:keys [body]} :parameters}]
  (let [context-type :string
        {ledger-id       const/iri-ledger
         txn-context     "@context"
         txn-opts        const/iri-opts
         default-context const/iri-default-context
         :as parsed-txn} (transact-api/parse-json-ld-txn conn context-type
                                                         body)
        _            (log/trace "parsed transact req:" parsed-txn)
        _exists?     (or (deref! (fluree/exists? conn ledger-id))
                         (throw-ledger-doesnt-exist ledger-id))
        opts*        (cond-> (or (:opts body) {})
                       txn-opts (merge txn-opts)
                       txn-context (assoc :txn-context txn-context)
                       default-context (assoc :default-context default-context))
        parsed-txn*  (assoc parsed-txn :opts opts*)
        resp-p       (promise)]

    (log/trace "transact handler default-context:" default-context)
    (log/trace "transact handler parsed-txn*:" parsed-txn*)
    ;; kick of async process that will eventually deliver resp or exception to resp-p
    (transact! resp-p consensus conn watcher parsed-txn*)

    {:status 200
     :body   (deref! resp-p)}))
