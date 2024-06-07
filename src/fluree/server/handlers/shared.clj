(ns fluree.server.handlers.shared
  (:require [clojure.core.async :refer [<! go]]
            [fluree.db.util.core :as util]
            [fluree.crypto :as crypto]
            [fluree.json-ld :as json-ld]
            [fluree.db.util.log :as log])
  (:import (clojure.lang ExceptionInfo)))

(set! *warn-on-reflection* true)

(defn deref!
  "Derefs promise p and throws if the result is an exception, returns it otherwise."
  [p]
  (let [res @p]
    (if (util/exception? res)
      (throw res)
      res)))

(defmacro defhandler
  [name args & body]
  `(defn ~name ~args
     (try
       ~@body
       (catch ExceptionInfo e#
         (if (-> e# ex-data (contains? :response))
           (throw e#)
           (let [msg#   (ex-message e#)
                 {status# :status :as data# :or {status# 500}} (ex-data e#)
                 error# (dissoc data# :status)]
             (throw (ex-info "Error in ledger handler"
                             {:response
                              {:status status#
                               :body   (assoc error# :message msg#)}})))))
       (catch Throwable t#
         (throw (ex-info "Error in ledger handler"
                         {:response {:status 500
                                     :body   {:error (ex-message t#)}}}))))))

(defn derive-tx-id
  [raw-txn]
  (if (string? raw-txn)
    (crypto/sha2-256 raw-txn :hex :string)
    (-> (json-ld/normalize-data raw-txn)
        (crypto/sha2-256 :hex :string))))

(defn wrap-consensus-response
  [ledger-id tx-id resp-ch]
  (let [p (promise)]
    (go
      (let [final-resp (<! resp-ch)]
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
                        :t      t
                        :tx-id  tx-id})))))
    p))
