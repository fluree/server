(ns fluree.server.integration.basic-transaction-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [fluree.server.integration.test-system
             :refer [api-post create-rand-ledger json-headers run-test-server]
             :as test-system]
            [jsonista.core :as json]))

(use-fixtures :once run-test-server)

(deftest ^:integration ^:json create-endpoint-json-test
  (testing "can create a new ledger w/ JSON"
    (let [ledger-name (str "create-endpoint-" (random-uuid))
          req         (json/write-value-as-string
                       {"ledger"   ledger-name
                        "@context" {"foo" "http://foobar.com/"}
                        "insert"   [{"id"      "ex:create-test"
                                     "type"    "foo:test"
                                     "ex:name" "create-endpoint-test"}]})
          res         (api-post :create {:body req :headers json-headers})]
      (is (= 201 (:status res)))
      (is (= {"ledger" ledger-name
              "t"      1}
             (-> res :body json/read-value (select-keys ["ledger" "t"])))))))

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
          req         (json/write-value-as-string
                       {"@context" test-system/default-context
                        "ledger" ledger-name
                        "insert" {"id"      "ex:transaction-test"
                                  "type"    "schema:Test"
                                  "ex:name" "transact-endpoint-json-test"}})
          res         (api-post :transact {:body req :headers json-headers})]
      (is (= 200 (:status res)))
      (is (= {"ledger" ledger-name, "t" 2}
             (-> res :body json/read-value (select-keys ["ledger" "t"])))))))

(deftest ^:integration ^:json transaction-with-dbs-at-multiple-t-values
  (testing "can transact with a where clause"
    (let [ledger-name (create-rand-ledger "transact-endpoint-multi-ledger")
          req1        (json/write-value-as-string
                        {"@context" test-system/default-context
                         "ledger"   ledger-name
                         "insert"   {"id"      "ex:transaction-test"
                                     "type"    "schema:Test"
                                     "ex:name" "transact-endpoint-json-test"}})
          res1        (api-post :transact {:body req1, :headers json-headers})
          _           (assert (= 200 (:status res1)))
          req2        (json/write-value-as-string
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
             (->> {:body    (json/write-value-as-string
                              {"@context" test-system/default-context
                               "select"   {"ex:transaction-test" ["*"]}
                               "from"     ledger-name})
                   :headers json-headers}
                  (api-post :query)
                  :body
                  json/read-value)))

      (is (= [{"id"      "ex:transaction-test"
               "type"    "schema:Test"
               "ex:name" "transact-endpoint-json-test"}]
             (->> {:body    (json/write-value-as-string
                              {"@context" test-system/default-context
                               "select"   {"ex:transaction-test" ["*"]}
                               "from"     (str ledger-name "?t=2")})
                   :headers json-headers}
                  (api-post :query)
                  :body
                  json/read-value))))))

(deftest ^:integration ^:json transaction-with-where-clause-test
  (testing "can transact with a where clause"
    (let [ledger-name (create-rand-ledger "transact-endpoint-json-test")
          req1        (json/write-value-as-string
                       {"@context" test-system/default-context
                        "ledger"   ledger-name
                        "insert"   {"id"      "ex:transaction-test"
                                    "type"    "schema:Test"
                                    "ex:name" "transact-endpoint-json-test"}})
          res1        (api-post :transact {:body req1, :headers json-headers})
          _           (assert (= 200 (:status res1)))
          req2        (json/write-value-as-string
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
             (->> {:body    (json/write-value-as-string
                             {"@context" test-system/default-context
                              "select"   {"ex:transaction-test" ["*"]}
                              "from"     ledger-name})
                   :headers json-headers}
                  (api-post :query)
                  :body
                  json/read-value)))))
  (testing "can transact with a where clause w/ optional"
    (let [ledger-name (create-rand-ledger "transact-endpoint-json-test")
          req1        (json/write-value-as-string
                       {"@context" test-system/default-context
                        "ledger"   ledger-name
                        "insert"   {"id"          "ex:transaction-test"
                                    "type"        "schema:Test"
                                    "schema:name" "transact-endpoint-json-test"}})
          res1        (api-post :transact {:body req1, :headers json-headers})
          _           (assert (= 200 (:status res1)))
          req2        (json/write-value-as-string
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
             (->> {:body    (json/write-value-as-string
                             {"@context" test-system/default-context
                              "select"   {"?t" ["*"]}
                              "from"     ledger-name
                              "where"    {"id" "?t", "type" "schema:Test"}})
                   :headers json-headers}
                  (api-post :query)
                  :body
                  json/read-value))))))

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
             (-> (api-post :transact {:body (json/write-value-as-string req1), :headers json-headers})
                 :status)))
      (is (= [{"id" "ex:list-test",
               "ex:items1" ["zero" "one" "two" "three"]
               "ex:items2" ["four" "five" "six" "seven"]}]
             (-> (api-post :query {:body (json/write-value-as-string q1) :headers json-headers})
                 :body
                 json/read-value)))
      (is (= 200
             (-> (api-post :transact {:body (json/write-value-as-string req2) :headers json-headers})
                 :status)))
      (is (= [{"id" "ex:list-test"}]
             (-> (api-post :query {:body (json/write-value-as-string q1) :headers json-headers})
                 :body
                 json/read-value))))))

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
