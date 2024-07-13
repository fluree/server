(ns fluree.server.integration.policy-test
  (:require [clojure.core.async :refer [<!!]]
            [clojure.test :refer [deftest is testing use-fixtures]]
            [fluree.crypto :as crypto]
            [fluree.db.json-ld.credential :as cred]
            [fluree.server.integration.test-system
             :as test-system
             :refer [api-post auth create-rand-ledger json-headers run-test-server]]
            [jsonista.core :as json]))

(use-fixtures :once run-test-server)

(deftest ^:integration ^:json policy-opts-json-test
  (testing "policy-enforcing opts are correctly handled"
    (let [ledger-name  (create-rand-ledger "policy-opts-test")
          alice-did    (:id auth)
          txn-req      {:body
                        (json/write-value-as-string
                         {"ledger"   ledger-name
                          "@context" {"ex"     "http://example.org/ns/"
                                      "schema" "http://schema.org/"
                                      "f"      "https://ns.flur.ee/ledger#"}
                          "insert"   [{"@id"              "ex:alice",
                                       "@type"            "ex:User",
                                       "schema:name"      "Alice"
                                       "schema:email"     "alice@flur.ee"
                                       "schema:birthDate" "2022-08-17"
                                       "schema:ssn"       "111-11-1111"}
                                      {"@id"              "ex:john",
                                       "@type"            "ex:User",
                                       "schema:name"      "John"
                                       "schema:email"     "john@flur.ee"
                                       "schema:birthDate" "2021-08-17"
                                       "schema:ssn"       "888-88-8888"}
                                      {"@id"                  "ex:widget",
                                       "@type"                "ex:Product",
                                       "schema:name"          "Widget"
                                       "schema:price"         99.99
                                       "schema:priceCurrency" "USD"}
                                      ;; assign alice-did to "ex:EmployeePolicy" and also link the did to "ex:alice" via "ex:user"
                                      {"@id"           alice-did
                                       "f:policyClass" [{"@id" "ex:EmployeePolicy"}]
                                       "ex:user"       {"@id" "ex:alice"}}
                                      ;; embedded policy
                                      {"@id"          "ex:ssnRestriction"
                                       "@type"        ["f:AccessPolicy" "ex:EmployeePolicy"]
                                       "f:onProperty" [{"@id" "schema:ssn"}]
                                       "f:action"     {"@id" "f:view"}
                                       "f:query"      {"@type"  "@json"
                                                       "@value" {"@context" {"ex" "http://example.org/ns/"}
                                                                 "where"    {"@id"     "?$identity"
                                                                             "ex:user" {"@id" "?$this"}}}}}]})
                        :headers json-headers}
          txn-res      (api-post :transact txn-req)
          _            (assert (= 200 (:status txn-res)))
          secret-query {"@context" {"ex"     "http://example.org/ns/"
                                    "schema" "http://schema.org/"}
                        "from"     ledger-name
                        "select"   ["?s" "?ssn"]
                        "where"    {"@id"        "?s"
                                    "@type"      "ex:User"
                                    "schema:ssn" "?ssn"}}
          query-req    {:body
                        (json/write-value-as-string
                         (assoc secret-query
                           "opts" {"did" alice-did}))
                        :headers json-headers}
          query-res    (api-post :query query-req)]

      (is (= 200 (:status query-res))
          (str "policy-enforced query response was: " (pr-str query-res)))

      (is (= [["ex:alice" "111-11-1111"]]
             (-> query-res :body json/read-value))
          "query policy opts should prevent seeing john's ssn")
      #_(let [txn-req   {:body
                         (json/write-value-as-string
                          {"@context" ["https://ns.flur.ee" test-system/default-context]
                           "ledger"   ledger-name
                           "delete"   [{"id"        "ex:alice"
                                        "ex:secret" "alice's secret"}]
                           "insert"   [{"id"        "ex:alice"
                                        "ex:secret" "alice's NEW secret"}]
                           "opts"     {"role" "ex:userRole"
                                       "did"  alice-did}})
                         :headers json-headers}
              txn-res   (api-post :transact txn-req)
              _         (assert (= 200 (:status txn-res)))
              query-req {:body
                         (json/write-value-as-string
                          secret-query)
                         :headers json-headers}
              query-res (api-post :query query-req)
              _         (assert (= 200 (:status query-res)))]
          (is (= #{{"id"        "ex:bob",
                    "type"      "ex:User",
                    "ex:secret" "bob's secret"}
                   {"id"        "ex:alice",
                    "type"      "ex:User",
                    "ex:secret" "alice's NEW secret"}}
                 (-> query-res :body json/read-value set))
              "alice's secret should be modified")
          (testing "plain requests"
            (let [txn-req {:body
                           (json/write-value-as-string
                            {"@context" ["https://ns.flur.ee" test-system/default-context]
                             "ledger"   ledger-name
                             "insert"   [{"id" "ex:bob"}
                                         "ex:secret" "bob's new secret"]
                             "opts"     {"role" "ex:userRole"
                                         "did"  alice-did}})
                           :headers json-headers}
                  txn-res (api-post :transact txn-req)]
              (is (not= 200 (:status txn-res))
                  "transaction policy opts prevented modification")
              (let [query-req {:body
                               (json/write-value-as-string
                                {"@context" test-system/default-context
                                 "from"     ledger-name
                                 "history"  "ex:bob"
                                 "t"        {"from" 1}
                                 "opts"     {"role" "ex:userRole"
                                             "did"  alice-did}})
                               :headers json-headers}
                    query-res (api-post :history query-req)]
                (is (= 200 (:status query-res)))
                (is (= [{"id" "ex:bob", "type" "ex:User"}]
                       (-> query-res :body json/read-value first (get "f:assert")))
                    "policy opts prevented seeing bob's secret"))))
          (testing "credential requests"
            (let [txn-req (<!! (cred/generate
                                {"@context" ["https://ns.flur.ee" test-system/default-context]
                                 "ledger"   ledger-name
                                 "insert"   [{"id" "ex:bob"}
                                             "ex:secret" "bob's new secret"]}
                                (:private auth)))
                  txn-res (api-post :transact {:body    (json/write-value-as-string txn-req)
                                               :headers json-headers})]
              (is (not= 200 (:status txn-res))
                  "transaction policy opts prevented modification")
              (let [query-req (<!! (cred/generate
                                    {"@context" test-system/default-context
                                     "from"     ledger-name
                                     "history"  "ex:bob"
                                     "t"        {"from" 1}}
                                    (:private auth)))
                    query-res (api-post :history {:body    (json/write-value-as-string query-req)
                                                  :headers json-headers})]
                (is (= 200 (:status query-res)))
                (is (= [{"id" "ex:bob", "type" "ex:User"}]
                       (-> query-res :body json/read-value first (get "f:assert")))
                    "policy opts prevented seeing bob's secret"))))
          (testing "JWS requests"
            (testing "authorized signer"
              (let [txn-req {"@context" ["https://ns.flur.ee" test-system/default-context]
                             "ledger"   ledger-name
                             "insert"   {"id"        "ex:alice"
                                         "ex:secret" "Alice's new secret"}}
                    txn-res (api-post :transact {:body    (crypto/create-jws
                                                           (json/write-value-as-string txn-req)
                                                           (:private auth))
                                                 :headers {"Content-Type" "application/jwt"}})]
                (is (= 200
                       (:status txn-res))
                    "txn signed by authorized user succeeds")))
            (testing "unauthorized signer"
              (let [txn-req {"@context" ["https://ns.flur.ee" test-system/default-context]
                             "ledger"   ledger-name
                             "insert"   [{"id" "ex:bob"}
                                         "ex:secret" "bob's new secret"]}
                    txn-res (api-post :transact {:body    (crypto/create-jws
                                                           (json/write-value-as-string txn-req)
                                                           (:private auth))
                                                 :headers {"Content-Type" "application/jwt"}})]
                (is (not= 200 (:status txn-res))
                    "transaction policy opts prevented modification")
                (is (= 400 (:status txn-res))
                    "transaction policy opts prevented modification")))
            (testing "query results filtered based on authorization"
              (let [query-req {"@context" test-system/default-context
                               "from"     ledger-name
                               "history"  "ex:bob"
                               "t"        {"from" 1}}
                    query-res (api-post :history {:body    (crypto/create-jws
                                                            (json/write-value-as-string query-req)
                                                            (:private auth))
                                                  :headers {"Content-Type" "application/jwt"}})]
                (is (= 200 (:status query-res)))
                (is (= [{"id" "ex:bob", "type" "ex:User"}]
                       (-> query-res :body json/read-value first (get "f:assert")))
                    "policy opts prevented seeing bob's secret"))))))))
