(ns fluree.server.integration.basic-transaction-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [fluree.db.util.json :as json]
            [fluree.server.integration.test-system
             :refer [api-post create-rand-ledger json-headers run-test-server]
             :as test-system]))

(use-fixtures :once run-test-server)

(deftest ^:integration ^:json create-endpoint-json-test
  (testing "can create a new ledger w/ JSON"
    (let [ledger-name (str "create-endpoint-" (random-uuid))
          req         (json/stringify
                       {"ledger"   ledger-name
                        "@context" {"foo" "http://foobar.com/"}
                        "insert"   [{"id"      "ex:create-test"
                                     "type"    "foo:test"
                                     "ex:name" "create-endpoint-test"}]})
          res         (api-post :create {:body req :headers json-headers})]
      (is (= 201 (:status res)))
      ;; Should return t: 1 for genesis commit + transaction commit
      (is (= {"ledger" ledger-name
              "t"      1}
             (-> res :body (json/parse false) (select-keys ["ledger" "t"])))))))

(deftest ^:integration ^:json create-endpoint-without-data-json-test
  (testing "can create a new ledger without initial data w/ JSON"
    (let [ledger-name (str "create-endpoint-no-data-" (random-uuid))
          req         (json/stringify
                       {"ledger" ledger-name})
          res         (api-post :create {:body req :headers json-headers})
          body        (json/parse (:body res) false)]
      (is (= 201 (:status res)))
      ;; Should return t: 0 for genesis commit only (no transaction data)
      (is (= {"ledger" ledger-name
              "t"      0}
             (select-keys body ["ledger" "t"])))
      ;; Should have same response structure as creation with data
      (is (contains? body "tx-id"))
      (is (contains? body "commit"))
      (is (contains? (get body "commit") "address"))
      (is (contains? (get body "commit") "hash")))))

#_(deftest ^:integration ^:edn create-endpoint-edn-test
    (testing "can create a new ledger w/ EDN"
      (let [ledger-name (str "create-endpoint-" (random-uuid))
            address     (str "fluree:memory://" ledger-name "/main/head")
            req         (pr-str {:ledger         ledger-name
                                 :defaultContext ["" {:foo "http://foobar.com/"}]
                                 :txn            [{:id      :ex/create-test
                                                   :type    :foo/test
                                                   :ex/name "create-endpoint-test"}]})
            res         (api-post :create {:body req :headers edn-headers})]
        (is (= 201 (:status res)))
        (is (= {:address address
                :alias   ledger-name
                :t       1}
               (-> res :body edn/read-string)))))

    (testing "responds with 409 error if ledger already exists"
      (let [ledger-name (str "create-endpoint-" (random-uuid))
            req         (pr-str {:ledger         ledger-name
                                 :defaultContext ["" {:foo "http://foobar.com/"}]
                                 :txn            [{:id      :ex/create-test
                                                   :type    :foo/test
                                                   :ex/name "create-endpoint-test"}]})
            res-success (api-post :create {:body req :headers edn-headers})
            _           (assert (= 201 (:status res-success)))
            res-fail    (api-post :create {:body req :headers edn-headers})]
        (is (= 409 (:status res-fail))))))

(deftest ^:integration ^:json transaction-json-test
  (testing "can transact in JSON"
    (let [ledger-name (create-rand-ledger "transact-endpoint-json-test")
          req         (json/stringify
                       {"@context" test-system/default-context
                        "ledger" ledger-name
                        "insert" {"id"      "ex:transaction-test"
                                  "type"    "schema:Test"
                                  "ex:name" "transact-endpoint-json-test"}})
          res         (api-post :update {:body req :headers json-headers})]
      (is (= 200 (:status res)))
      (is (= {"ledger" ledger-name, "t" 2}
             (-> res :body (json/parse false) (select-keys ["ledger" "t"])))))))

(deftest ^:integration ^:json transaction-with-where-clause-test
  (testing "can transact with a where clause"
    (let [ledger-name (create-rand-ledger "transact-endpoint-json-test")
          req1        (json/stringify
                       {"@context" test-system/default-context
                        "ledger"   ledger-name
                        "insert"   {"id"      "ex:transaction-test"
                                    "type"    "schema:Test"
                                    "ex:name" "transact-endpoint-json-test"}})
          res1        (api-post :transact {:body req1, :headers json-headers})
          _           (assert (= 200 (:status res1)))
          req2        (json/stringify
                       {"@context" test-system/default-context
                        "ledger"   ledger-name
                        "insert"   {"id"      "?t"
                                    "ex:name" "new-name"}
                        "delete"   {"id"      "?t"
                                    "ex:name" "transact-endpoint-json-test"}
                        "where"    {"id" "?t", "type" "schema:Test"}})
          res2        (api-post :transact {:body req2, :headers json-headers})]
      (is (= 200 (:status res2)))
      (is (= [{"id"      "ex:transaction-test"
               "type"    "schema:Test"
               "ex:name" "new-name"}]
             (-> (api-post :query {:body    (json/stringify
                                             {"@context" test-system/default-context
                                              "select"   {"ex:transaction-test" ["*"]}
                                              "from"     ledger-name})
                                   :headers json-headers})

                 :body
                 (json/parse false))))))
  (testing "can transact with a where clause w/ optional"
    (let [ledger-name (create-rand-ledger "transact-endpoint-json-test")
          req1        (json/stringify
                       {"@context" test-system/default-context
                        "ledger"   ledger-name
                        "insert"   {"id"          "ex:transaction-test"
                                    "type"        "schema:Test"
                                    "schema:name" "transact-endpoint-json-test"}})
          res1        (api-post :transact {:body req1, :headers json-headers})
          _           (assert (= 200 (:status res1)))
          req2        (json/stringify
                       {"@context" test-system/default-context
                        "ledger"   ledger-name
                        "insert"   {"id"          "?t"
                                    "schema:name" "new-name"}
                        "delete"   {"id"                 "?t"
                                    "schema:name"        "?n"
                                    "schema:description" "?d"}
                        "where"    [{"id" "?t", "type" "schema:Test"}
                                    ["optional" {"id"          "?t"
                                                 "schema:name" "?n"}]
                                    ["optional" {"id"                 "?t"
                                                 "schema:description" "?d"}]]})
          res2        (api-post :transact {:body req2, :headers json-headers})]
      (is (= 200 (:status res2)))
      (is (= [{"id"          "ex:transaction-test"
               "type"        "schema:Test"
               "schema:name" "new-name"}]
             (-> (api-post :query {:body    (json/stringify
                                             {"@context" test-system/default-context
                                              "select"   {"?t" ["*"]}
                                              "from"     ledger-name
                                              "where"    {"id" "?t", "type" "schema:Test"}})
                                   :headers json-headers})
                 :body
                 (json/parse false)))))))

(deftest ^:integration ^:json transaction-with-ordered-list
  (testing ""
    (let [ledger-name (create-rand-ledger "transact-endpoint-json-test")
          req1        {"@context" [test-system/default-context {"ex:items2" {"@container" "@list"}}]
                       "ledger"   ledger-name
                       "insert"   {"id"        "ex:list-test"
                                   "ex:items1" {"@list" ["zero" "one" "two" "three"]}
                                   "ex:items2" ["four" "five" "six" "seven"]}}

          q1 {"@context" test-system/default-context
              "from"     ledger-name
              "select"   {"ex:list-test" ["*"]}}

          req2   {"@context" [test-system/default-context {"ex:items2" {"@container" "@list"}}]
                  "ledger" ledger-name
                  "where" {"@id" "ex:list-test" "ex:items1" "?items1" "ex:items2" "?items2"}
                  "delete" {"id" "ex:list-test"
                            "ex:items1" "?items1"
                            "ex:items2" "?items2"}}]
      (is (= 200
             (-> (api-post :transact {:body (json/stringify req1), :headers json-headers})
                 :status)))
      (is (= [{"id" "ex:list-test",
               "ex:items1" ["zero" "one" "two" "three"]
               "ex:items2" ["four" "five" "six" "seven"]}]
             (-> (api-post :query {:body (json/stringify q1) :headers json-headers})
                 :body
                 (json/parse false))))
      (is (= 200
             (-> (api-post :transact {:body (json/stringify req2) :headers json-headers})
                 :status)))
      (is (= [{"id" "ex:list-test"}]
             (-> (api-post :query {:body (json/stringify q1) :headers json-headers})
                 :body
                 (json/parse false)))))))

(deftest upsert-test
  (let [ledger-name (create-rand-ledger "upsert-test")

        insert {"@context" {"ex" "http://example.org/ns/"
                            "schema" "http://schema.org/"}
                "@graph" [{"@id" "ex:alice",
                           "@type" "ex:User",
                           "schema:name" "Alice"
                           "schema:age" 42}
                          {"@id" "ex:bob",
                           "@type" "ex:User",
                           "schema:name" "Bob"
                           "schema:age" 22}]}

        upsert {"@context" {"ex" "http://example.org/ns/"
                            "schema" "http://schema.org/"}
                "@graph" [{"@id" "ex:alice"
                           "schema:name" "Alice2"}
                          {"@id" "ex:bob"
                           "schema:name" "Bob2"}
                          {"@id" "ex:jane"
                           "schema:name" "Jane2"}]}

        query {"@context" {"ex" "http://example.org/ns/"
                           "schema" "http://schema.org/"}
               "from" ledger-name
               "select" {"?id" ["*"]}
               "where" {"@id" "?id"
                        "schema:name" "?name"}}]
    (is (= 200
           (-> (api-post :insert {:body (json/stringify insert)
                                  :headers (-> json-headers
                                               (assoc "fluree-ledger" ledger-name))})
               :status)))
    (is (= 200
           (-> (api-post :upsert {:body (json/stringify upsert)
                                  :headers (-> json-headers
                                               (assoc "fluree-ledger" ledger-name))})
               :status)))
    (is (= [{"@id" "ex:alice",
             "@type" "ex:User",
             "schema:age" 42,
             "schema:name" "Alice2"}
            {"@id" "ex:bob",
             "schema:age" 22,
             "schema:name" "Bob2",
             "@type" "ex:User"}
            {"@id" "ex:jane",
             "schema:name" "Jane2"}]
           (-> (api-post :query {:body (json/stringify query) :headers json-headers})
               :body (json/parse false))))))

#_(deftest ^:integration ^:edn transaction-edn-test
    (testing "can transact in EDN"
      (let [ledger-name (create-rand-ledger "transact-endpoint-edn-test")
            address     (str "fluree:memory://" ledger-name "/main/head")
            req         (pr-str
                         {:ledger ledger-name
                          :txn    [{:id      :ex/transaction-test
                                    :type    :schema/Test
                                    :ex/name "transact-endpoint-edn-test"}]})
            res         (api-post :transact {:body req :headers edn-headers})]
        (is (= 200 (:status res)))
        (is (= {:address address, :alias ledger-name, :t 2}
               (-> res :body edn/read-string))))))
