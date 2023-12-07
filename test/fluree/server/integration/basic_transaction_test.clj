(ns fluree.server.integration.basic-transaction-test
  (:require [clojure.edn :as edn]
            [clojure.test :refer :all]
            [fluree.server.integration.test-system :refer :all]
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
                       {"ledger" ledger-name
                        "insert" {"id"      "ex:transaction-test"
                                  "type"    "schema:Test"
                                  "ex:name" "transact-endpoint-json-test"}})
          res         (api-post :transact {:body req :headers json-headers})]
      (is (= 200 (:status res)))
      (is (= {"ledger" ledger-name, "t" 2}
             (-> res :body json/read-value (select-keys ["ledger" "t"])))))))

(deftest ^:integration ^:json transaction-with-where-clause-test
  (testing "can transact with a where clause"
    (let [ledger-name (create-rand-ledger "transact-endpoint-json-test")
          req1        (json/write-value-as-string
                       {"ledger" ledger-name
                        "insert" {"id"      "ex:transaction-test"
                                  "type"    "schema:Test"
                                  "ex:name" "transact-endpoint-json-test"}})
          res1        (api-post :transact {:body req1, :headers json-headers})
          _           (assert (= 200 (:status res1)))
          req2        (json/write-value-as-string
                       {"ledger" ledger-name
                        "insert" {"id"      "?t"
                                  "ex:name" "new-name"}
                        "delete" {"id"      "?t"
                                  "ex:name" "transact-endpoint-json-test"}
                        "where"  {"id" "?t", "type" "schema:Test"}})
          res2        (api-post :transact {:body req2, :headers json-headers})]
      (is (= 200 (:status res2)))
      (is (= [{"id"      "ex:transaction-test"
               "type"    "schema:Test"
               "ex:name" "new-name"}]
             (->> {:body    (json/write-value-as-string
                             {"select" {"ex:transaction-test" ["*"]}
                              "from"   ledger-name})
                   :headers json-headers}
                  (api-post :query)
                  :body
                  json/read-value)))))
  (testing "can transact with a where clause w/ optional"
    (let [ledger-name (create-rand-ledger "transact-endpoint-json-test")
          req1        (json/write-value-as-string
                       {"ledger" ledger-name
                        "insert" {"id"          "ex:transaction-test"
                                  "type"        "schema:Test"
                                  "schema:name" "transact-endpoint-json-test"}})
          res1        (api-post :transact {:body req1, :headers json-headers})
          _           (assert (= 200 (:status res1)))
          req2        (json/write-value-as-string
                       {"ledger" ledger-name
                        "insert" {"id"          "?t"
                                  "schema:name" "new-name"}
                        "delete" {"id"                 "?t"
                                  "schema:name"        "?n"
                                  "schema:description" "?d"}
                        "where"  [{"id" "?t", "type" "schema:Test"}
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
                             {"select" {"?t" ["*"]}
                              "from"   ledger-name
                              "where"  {"id" "?t", "type" "schema:Test"}})
                   :headers json-headers}
                  (api-post :query)
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
