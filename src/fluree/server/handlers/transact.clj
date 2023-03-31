(ns fluree.server.handlers.transact
  (:require [clojure.core.async :as async]
            [fluree.crypto :as crypto]
            [fluree.db.conn.proto :as conn-proto]
            [fluree.db.json-ld.api :as fluree]
            [fluree.json-ld :as json-ld]
            [fluree.raft :as raft]
            [fluree.http-api.handlers.ledger :refer [error-catching-handler deref! txn-body->opts]]
            [fluree.db.util.log :as log]
            [fluree.server.consensus.core :as consensus]))

(set! *warn-on-reflection* true)

(defn normalize-txn
  "Transactions are decoded, re-normalizes them for consistent hashing
  to ensure a consistent tx-id is produced."
  [ledger raw-txn]
  (json-ld/normalize-data
    {"ledger" ledger
     "txn"    raw-txn}
    {:algorithm :basic
     :format    :application/json}))

(defn hash-txn
  "Hashes a string, JSON representation of a txn that should
  already be normalized."
  [json-txn]
  (crypto/sha2-256 json-txn :hex :string))


(defn persist-transaction
  [consensus conn ledger raw-txn]
  (try
    (let [normalized  (normalize-txn ledger raw-txn)
          tx-id       (hash-txn normalized)
          conn-method (conn-proto/-method conn)
          p           (promise)]
      (async/go
        (deliver p
                 (async/<!
                   (consensus/queue-new-transaction consensus
                                                    conn-method
                                                    ledger
                                                    tx-id
                                                    normalized))))
      p)
    (catch Exception e
      (log/error e "Error persisting transaction to consensus!")
      (throw (ex-info (str "Error persisting transaction through consensus with error: " (ex-message e))
                      {:status 500 :error :db/consensus})))))


(defn default
  [{:keys [:fluree/consensus] :as _cfg}]
  (error-catching-handler
    (fn [{:keys [:fluree/conn] {{:keys [ledger txn] :as body} :body} :parameters}]
      (let [ledger-exists? (deref! (fluree/exists? conn ledger))]
        (log/debug "Ledger" ledger "exists?" ledger-exists?)
        (if ledger-exists?
          (do
            (log/info "Creating ledger" ledger)
            (let [persist-resp (deref! (persist-transaction consensus conn ledger txn))
                  http-resp    (select-keys persist-resp [:tx-id :size :ledger-id :instant])]
              {:status 201
               :body   http-resp}))
          (let [err-message (str "Ledger " ledger " does not exist!")]
            (throw (ex-info err-message
                            {:response {:status 409
                                        :body   {:error err-message}}}))))))))

