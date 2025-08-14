(ns fluree.server.integration.policy-test
  (:require [clojure.core.async :refer [<!!]]
            [clojure.test :refer [deftest is testing use-fixtures]]
            [fluree.crypto :as crypto]
            [fluree.db.json-ld.credential :as cred]
            [fluree.db.util.json :as json]
            [fluree.server.integration.test-system
             :as test-system
             :refer [api-post auth create-rand-ledger json-headers sparql-headers run-test-server]]))

(use-fixtures :once run-test-server)

(deftest ^:integration ^:json policy-opts-json-test
  (testing "policy-enforcing opts are correctly handled"
    (let [ledger-name  (create-rand-ledger "policy-opts-test")
          alice-did    (:id auth)
          txn-req      {:body
                        (json/stringify
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
                                      {"@id"            "ex:ssnRestriction"
                                       "@type"          ["f:AccessPolicy" "ex:EmployeePolicy"]
                                       "f:targetProperty" [{"@id" "schema:ssn"}]
                                       "f:required"     true
                                       "f:action"       [{"@id" "f:view"} {"@id" "f:modify"}]
                                       "f:query"        {"@type"  "@json"
                                                         "@value" {"@context" {"ex" "http://example.org/ns/"}
                                                                   "where"    {"@id"     "?$identity"
                                                                               "ex:user" {"@id" "?$this"}}}}}
                                      {"@id"      "ex:defaultAllowView"
                                       "@type"    ["f:AccessPolicy" "ex:EmployeePolicy"]
                                       "f:action" {"@id" "f:view"}
                                       "f:query"  {"@type"  "@json"
                                                   "@value" {}}}]})
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
                        (json/stringify
                         (assoc secret-query
                                "opts" {"did" alice-did}))
                        :headers json-headers}
          query-res    (api-post :query query-req)]

      (is (= 200 (:status query-res))
          (str "policy-enforced query response was: " (pr-str query-res)))

      (is (= [["ex:alice" "111-11-1111"]]
             (-> query-res :body (json/parse false)))
          "query policy opts should prevent seeing john's ssn")

      (let [txn-req   {:body
                       (json/stringify
                        {"@context" {"ex"     "http://example.org/ns/"
                                     "schema" "http://schema.org/"
                                     "f"      "https://ns.flur.ee/ledger#"}
                         "ledger"   ledger-name
                         "delete"   [{"@id"        "ex:alice"
                                      "schema:ssn" "111-11-1111"}]
                         "insert"   [{"@id"        "ex:alice"
                                      "schema:ssn" "222-22-2222"}]
                         "opts"     {"did" alice-did}})
                       :headers json-headers}
            txn-res   (api-post :transact txn-req)
            _         (assert (= 200 (:status txn-res)))
            query-req {:body
                       (json/stringify
                        (assoc secret-query
                               "opts" {"did" alice-did}))
                       :headers json-headers}
            query-res (api-post :query query-req)
            _         (assert (= 200 (:status query-res)))]

        (is (= [["ex:alice" "222-22-2222"]]
               (-> query-res :body (json/parse false)))
            "alice's secret should be modified")

        (testing "plain requests"
          (let [txn-req {:body
                         (json/stringify
                          {"@context" {"ex"     "http://example.org/ns/"
                                       "schema" "http://schema.org/"
                                       "f"      "https://ns.flur.ee/ledger#"}
                           "ledger"   ledger-name
                           "insert"   [{"@id"        "ex:john"
                                        "schema:ssn" "888-88-8888"}]
                           "opts"     {"did" alice-did}})
                         :headers json-headers}
                txn-res (api-post :transact txn-req)]

            (is (not= 200 (:status txn-res))
                "transaction policy opts prevented modification")

            (let [query-req {:body
                             (json/stringify
                              {"@context" {"ex"     "http://example.org/ns/"
                                           "schema" "http://schema.org/"
                                           "f"      "https://ns.flur.ee/ledger#"}
                               "from"     ledger-name
                               "history"  "ex:john"
                               "t"        {"from" 1}
                               "opts"     {"did" alice-did}})
                             :headers json-headers}
                  query-res (api-post :history query-req)]

              (is (= 200 (:status query-res)))

              (is (= [{"@id"              "ex:john"
                       "@type"            "ex:User"
                       "schema:birthDate" "2021-08-17"
                       "schema:email"     "john@flur.ee"
                       "schema:name"      "John"}]
                     (-> query-res :body (json/parse false) first (get "f:assert")))
                  "policy opts prevented seeing john's ssn"))))

        (testing "credential requests"
          (let [txn-req (<!! (cred/generate
                              {"@context" {"ex"     "http://example.org/ns/"
                                           "schema" "http://schema.org/"
                                           "f"      "https://ns.flur.ee/ledger#"}
                               "ledger"   ledger-name
                               "insert"   [{"@id"        "ex:john"
                                            "schema:ssn" "999-99-9999"}]}
                              (:private auth)))
                txn-res (api-post :transact {:body    (json/stringify txn-req)
                                             :headers json-headers})]

            (is (not= 200 (:status txn-res))
                "transaction policy opts prevented modification")

            (let [query-req (<!! (cred/generate
                                  {"@context" {"ex"     "http://example.org/ns/"
                                               "schema" "http://schema.org/"
                                               "f"      "https://ns.flur.ee/ledger#"}
                                   "from"     ledger-name
                                   "history"  "ex:john"
                                   "t"        {"from" 1}}
                                  (:private auth)))
                  query-res (api-post :history {:body    (json/stringify query-req)
                                                :headers json-headers})]

              (is (= 200 (:status query-res)))

              (is (= [{"@id"              "ex:john"
                       "@type"            "ex:User"
                       "schema:birthDate" "2021-08-17"
                       "schema:email"     "john@flur.ee"
                       "schema:name"      "John"}]
                     (-> query-res :body (json/parse false) first (get "f:assert")))
                  "policy opts prevented seeing john's ssn"))))

        (testing "JWS requests"
          (testing "authorized signer"
            (let [txn-req {"@context" {"ex"     "http://example.org/ns/"
                                       "schema" "http://schema.org/"
                                       "f"      "https://ns.flur.ee/ledger#"}
                           "ledger"   ledger-name
                           "insert"   {"@id"        "ex:alice"
                                       "schema:ssn" "444-44-4444"}}
                  txn-res (api-post :transact {:body    (crypto/create-jws
                                                         (json/stringify txn-req)
                                                         (:private auth))
                                               :headers {"Content-Type" "application/jwt"}})]
              (is (= 200
                     (:status txn-res))
                  "txn signed by authorized user succeeds")))

          (testing "unauthorized signer"
            (let [txn-req {"@context" {"ex"     "http://example.org/ns/"
                                       "schema" "http://schema.org/"
                                       "f"      "https://ns.flur.ee/ledger#"}
                           "ledger"   ledger-name
                           "insert"   [{"@id"        "ex:john"
                                        "schema:ssn" "333-33-3333"}]}
                  txn-res (api-post :transact {:body    (crypto/create-jws
                                                         (json/stringify txn-req)
                                                         (:private auth))
                                               :headers {"Content-Type" "application/jwt"}})]

              (is (not= 200 (:status txn-res))
                  "transaction policy opts prevented modification")

              (is (= 403 (:status txn-res))
                  "transaction policy opts prevented modification")))

          (testing "query results filtered based on authorization"
            (let [query-req {"@context" {"ex"     "http://example.org/ns/"
                                         "schema" "http://schema.org/"
                                         "f"      "https://ns.flur.ee/ledger#"}
                             "from"     ledger-name
                             "history"  "ex:john"
                             "t"        {"from" 1}}
                  query-res (api-post :history {:body    (crypto/create-jws
                                                          (json/stringify query-req)
                                                          (:private auth))
                                                :headers {"Content-Type" "application/jwt"}})]

              (is (= 200 (:status query-res)))

              (is (= [{"@id"              "ex:john"
                       "@type"            "ex:User"
                       "schema:birthDate" "2021-08-17"
                       "schema:email"     "john@flur.ee"
                       "schema:name"      "John"}]
                     (-> query-res :body (json/parse false) first (get "f:assert")))
                  "policy opts prevented seeing john's secret"))))))))

(deftest ^:integration ^:json policy-class-opts-test
  (testing "policy-enforcing opts for policyClass are correctly handled"
    (let [ledger-name  (create-rand-ledger "policy-class-opts-test")
          alice-did    (:id auth)
          txn-req      {:body
                        (json/stringify
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
                                      {"@id"            "ex:ssnRestriction"
                                       "@type"          ["f:AccessPolicy" "ex:EmployeePolicy"]
                                       "f:targetProperty" [{"@id" "schema:ssn"}]
                                       "f:action"       [{"@id" "f:view"} {"@id" "f:modify"}]
                                       "f:required"     true
                                       "f:query"        {"@type"  "@json"
                                                         "@value" {"@context" {"ex" "http://example.org/ns/"}
                                                                   "where"    {"@id"     "?$identity"
                                                                               "ex:user" {"@id" "?$this"}}}}}
                                      {"@id"      "ex:defaultAllowView"
                                       "@type"    ["f:AccessPolicy" "ex:EmployeePolicy"]
                                       "f:action" {"@id" "f:view"}
                                       "f:query"  {"@type"  "@json"
                                                   "@value" {}}}]})
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
                        (json/stringify
                         (assoc secret-query
                                "opts" {"policyClass"  "ex:EmployeePolicy"
                                        "policyValues" ["?$identity" [{"@type" "@id" "@value" alice-did}]]}))
                        :headers json-headers}
          query-res    (api-post :query query-req)]

      (is (= 200 (:status query-res))
          (str "policy-enforced query response was: " (pr-str query-res)))

      (is (= [["ex:alice" "111-11-1111"]]
             (-> query-res :body (json/parse false)))
          "query policy opts should prevent seeing john's ssn")

      (testing "same query but with SPARQL and policy headers"
        (let [query-req {:body    (str "PREFIX schema: <http://schema.org/>
                                        PREFIX ex: <http://example.org/ns/>
                                        SELECT ?s ?ssn
                                        FROM   <" ledger-name ">
                                        WHERE  {?s a ex:User;
                                                   schema:ssn ?ssn.}")
                         :headers (assoc sparql-headers
                                         "Fluree-Policy-Class" "ex:EmployeePolicy"
                                         "Fluree-Policy-Values" (json/stringify ["?$identity" [{"@type" "@id" "@value" alice-did}]]))}
              query-res (api-post :query query-req)]

          (is (= [["ex:alice" "111-11-1111"]]
                 (-> query-res :body (json/parse false)))
              "query policy opts should prevent seeing john's ssn"))))))

(deftest ^:integration ^:json policy-json-ld-opts-test
  (testing "policy-enforcing opts for json-ld policy are correctly handled"
    (let [ledger-name   (create-rand-ledger "policy-json-ld-opts-test")
          alice-did     (:id auth)
          txn-req       {:body
                         (json/stringify
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
                                       {"@id"     alice-did
                                        "ex:user" {"@id" "ex:alice"}}]})
                         :headers json-headers}
          txn-res       (api-post :transact txn-req)
          _             (assert (= 200 (:status txn-res)))
          secret-query  {"@context" {"ex"     "http://example.org/ns/"
                                     "schema" "http://schema.org/"}
                         "from"     ledger-name
                         "select"   ["?s" "?ssn"]
                         "where"    {"@id"        "?s"
                                     "@type"      "ex:User"
                                     "schema:ssn" "?ssn"}}
          policy        {"@context" {"ex"     "http://example.org/ns/"
                                     "schema" "http://schema.org/"
                                     "f"      "https://ns.flur.ee/ledger#"}
                         "@graph"   [{"@id"            "ex:ssnRestriction"
                                      "@type"          ["f:AccessPolicy" "ex:EmployeePolicy"]
                                      "f:targetProperty" [{"@id" "schema:ssn"}]
                                      "f:action"       [{"@id" "f:view"} {"@id" "f:modify"}]
                                      "f:required"     true
                                      "f:query"        {"@type"  "@json"
                                                        "@value" {"@context" {"ex" "http://example.org/ns/"}
                                                                  "where"    {"@id"     "?$identity"
                                                                              "ex:user" {"@id" "?$this"}}}}}
                                     {"@id"      "ex:defaultAllowView"
                                      "@type"    ["f:AccessPolicy" "ex:EmployeePolicy"]
                                      "f:action" {"@id" "f:view"}
                                      "f:query"  {"@type"  "@json"
                                                  "@value" {}}}]}
          policy-values ["?$identity" [{"@type" "@id" "@value" alice-did}]]
          query-req     {:body
                         (json/stringify
                          (assoc secret-query
                                 "opts" {"policy"       policy
                                         "policyValues" policy-values}))
                         :headers json-headers}
          query-res     (api-post :query query-req)]

      (is (= 200 (:status query-res))
          (str "policy-enforced query response was: " (pr-str query-res)))

      (is (= [["ex:alice" "111-11-1111"]]
             (-> query-res :body (json/parse false)))
          "query policy opts should prevent seeing john's ssn")

      (testing "Same query but in SPARQL with policy http headers"
        (let [query-req {:body    (str "PREFIX schema: <http://schema.org/>
                                        PREFIX ex: <http://example.org/ns/>
                                        SELECT ?s ?ssn
                                        FROM   <" ledger-name ">
                                        WHERE  {?s a ex:User;
                                                   schema:ssn ?ssn.}")
                         :headers (assoc sparql-headers
                                         "Fluree-Policy" (json/stringify policy)
                                         "Fluree-Policy-Values" (json/stringify policy-values))}
              query-res (api-post :query query-req)]

          (is (= [["ex:alice" "111-11-1111"]]
                 (-> query-res :body (json/parse false)))
              "query policy opts should prevent seeing john's ssn"))))))
