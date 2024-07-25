(ns http-calls
  (:require [clj-http.client :as client]
            [fluree.db.util.json :as json]
            [fluree.server.consensus.network.multi-addr :refer [multi->map]]
            [user :as user]))


(def ledger-name "my/test")

(def server-1-address "http://localhost:58090/")
(def server-2-address "http://localhost:58091/")
(def server-3-address "http://localhost:58092/")


(comment

 (-> (client/get (str server-1-address "swagger.json"))
     :body
     (json/parse false))

 (-> (client/post (str server-1-address "fluree/create")
                  {:body               (json/stringify-UTF8
                                        {
                                         "@context" {"ex" "http://example.org/"}
                                         "ledger"   ledger-name
                                         "insert"   {"@id"     "ex:test1"
                                                     "ex:name" "Brian"}})
                   ;:headers {"X-Api-Version" "2"}
                   :content-type       :json
                   :socket-timeout     1000 ;; in milliseconds
                   :connection-timeout 1000 ;; in milliseconds
                   :accept             :json}))

 (-> (client/post (str server-1-address "fluree/transact")
                  {:body               (json/stringify-UTF8
                                        {"@context" {"ex" "http://example.org/"}
                                         "ledger"   ledger-name
                                         "insert"   {"@id"     "ex:test2"
                                                     "ex:name" "Brian2"}})
                   ;:headers {"X-Api-Version" "2"}
                   :content-type       :json
                   :socket-timeout     1000 ;; in milliseconds
                   :connection-timeout 1000 ;; in milliseconds
                   :accept             :json}))

 (-> (client/post (str server-1-address "fluree/query")
                  {:body               (json/stringify-UTF8
                                        {"@context" {"ex" "http://example.org/"}
                                         "select"   {"?s" ["*"]}
                                         "from"     ledger-name
                                         "where"    {"@id"     "?s"
                                                     "ex:name" nil}})
                   ;:headers {"X-Api-Version" "2"}
                   :content-type       :json
                   :socket-timeout     1000 ;; in milliseconds
                   :connection-timeout 1000 ;; in milliseconds
                   :accept             :json})
     :body
     (json/parse false))

 )