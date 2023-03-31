(ns http-calls
  (:require [clj-http.client :as client]
            [fluree.db.util.json :as json]))

(def ledger-name "my/test1")

(comment

  (-> (client/get "http://localhost:8091/swagger.json")
      :body
      (json/parse false))

  (-> (client/post "http://localhost:8091/fluree/create"
                   {:body               (json/stringify-UTF8
                                          {"action" "new"
                                           "ledger" ledger-name
                                           "txn"    {"id"      "ex:test1"
                                                     "ex:name" "Brian"}})
                    ;:headers {"X-Api-Version" "2"}
                    :content-type       :json
                    :socket-timeout     1000 ;; in milliseconds
                    :connection-timeout 1000 ;; in milliseconds
                    :accept             :json}))

  (-> (client/post "http://localhost:8091/fluree/transact"
                   {:body               (json/stringify-UTF8
                                          {"ledger" ledger-name
                                           "txn"    {"id"      "ex:test2"
                                                     "ex:name" "Brian2"}})
                    ;:headers {"X-Api-Version" "2"}
                    :content-type       :json
                    :socket-timeout     1000 ;; in milliseconds
                    :connection-timeout 1000 ;; in milliseconds
                    :accept             :json}))

  (-> (client/post "http://localhost:8093/fluree/query"
                   {:body               (json/stringify-UTF8
                                          {"ledger" ledger-name
                                           "query"  {"select" {"?s" ["*"]}
                                                     "where"  [["?s" "ex:name" nil]]}})
                    ;:headers {"X-Api-Version" "2"}
                    :content-type       :json
                    :socket-timeout     1000 ;; in milliseconds
                    :connection-timeout 1000 ;; in milliseconds
                    :accept             :json})
      :body
      (json/parse false))

  )