(ns fluree.server.handlers.create
  (:require
    [clojure.core.async :as async]
    [fluree.db.json-ld.api :as fluree]
    [fluree.db.util.log :as log]
    [fluree.db.conn.proto :as conn-proto]
    [fluree.json-ld :as json-ld]
    [fluree.server.consensus.core :as consensus]
    [fluree.server.handlers.transact :refer [hash-txn]]
    [fluree.http-api.handlers.ledger :refer [error-catching-handler deref!]]))

(set! *warn-on-reflection* true)

(defn persist-create-ledger
  "Persists a create-ledger request."
  [consensus conn {:keys [ledger context txn]}]
  (try
    (let [txn*  (json-ld/normalize-data
                        txn
                        {:algorithm :basic
                         :format    :application/json})
          tx-id       (hash-txn txn*)
          conn-method (conn-proto/-method conn)
          p           (promise)]
      (async/go
        (deliver p
                 (async/<!
                   (consensus/queue-new-ledger consensus
                                               conn-method
                                               ledger
                                               tx-id
                                               txn*
                                               context))))
      p)
    (catch Exception e
      (log/error e "Error persisting transaction to consensus!")
      (throw (ex-info (str "Error persisting transaction through consensus with error: " (ex-message e))
                      {:status 500 :error :db/consensus})))))


(defn default
  [{:keys [:fluree/consensus] :as _cfg}]
  (error-catching-handler
    (fn [{:keys [:fluree/conn] {{:keys [ledger] :as body} :body} :parameters}]
      (let [ledger-exists? (deref! (fluree/exists? conn ledger))]
        (log/debug "Ledger" ledger "exists?" ledger-exists?)
        (if ledger-exists?
          (let [err-message (str "Ledger " ledger " already exists")]
            (throw (ex-info err-message
                            {:response {:status 409
                                        :body   {:error err-message}}})))
          (do
            (log/info "Creating ledger" ledger)
            (let [persist-resp (deref! (persist-create-ledger consensus conn body))
                  http-resp    (select-keys persist-resp [:tx-id :size :ledger-id :instant])]
              {:status 201
               :body   http-resp})))))))
