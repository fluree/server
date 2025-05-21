(ns fluree.server.integration.credential-test
  (:require [clojure.core.async :refer [<!!]]
            [clojure.test :as test :refer [deftest testing is]]
            [fluree.db.json-ld.credential :as cred]
            [fluree.db.util.json :as json]
            [fluree.server.integration.test-system
             :refer [api-post auth json-headers run-test-server]]))

(test/use-fixtures :once run-test-server)

(def default-context
  {"id"     "@id"
   "type"   "@type"
   "graph"  "@graph"
   "xsd"    "http://www.w3.org/2001/XMLSchema#"
   "rdf"    "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
   "rdfs"   "http://www.w3.org/2000/01/rdf-schema#"
   "sh"     "http://www.w3.org/ns/shacl#"
   "schema" "http://schema.org/"
   "skos"   "http://www.w3.org/2008/05/skos#"
   "wiki"   "https://www.wikidata.org/wiki/"
   "f"      "https://ns.flur.ee/ledger#"
   "ex"     "http://example.com/ns/"})

(deftest ^:integration credential-test
  (let [ledger-name "credential-test"]
    (testing "create"
      (let [create-req {"ledger"   ledger-name
                        "@context" default-context
                        "insert"   {"@graph"
                                    [{"id"            (:id auth)
                                      "f:policyClass" {"id" "ex:RootPolicy"}
                                      "type"          "schema:Person"
                                      "ex:name"       "Goose"}
                                     {"@id"      "ex:defaultAllowView"
                                      "@type"    ["f:AccessPolicy" "ex:RootPolicy"]
                                      "f:action" [{"@id" "f:view"} {"@id" "f:modify"}]
                                      "f:query"  {"@type"  "@json"
                                                  "@value" {}}}]}}
            create-res (api-post :create
                                 {:body    (json/stringify create-req)
                                  :headers json-headers})]
        (is (= 201 (:status create-res)))
        (is (= {"ledger" "credential-test"
                "t"      1}
               (-> create-res :body (json/parse false) (select-keys ["ledger" "t"]))))))
    (testing "transact"
      (let [txn-req (<!! (cred/generate
                          {"ledger"   ledger-name
                           "@context" default-context
                           "insert"   [{"id"      "ex:cred-test"
                                        "type"    "schema:Test"
                                        "ex:name" "cred test"
                                        "ex:foo"  1}]}
                          (:private auth)))
            txn-res (api-post :transact
                              {:body    (json/stringify txn-req)
                               :headers json-headers})]
        (is (= 200 (:status txn-res)))
        (is (= {"ledger" "credential-test"
                "t"      2}
               (-> txn-res :body (json/parse false) (select-keys ["ledger" "t"]))))))
    (testing "query"
      (let [query-req (<!! (cred/generate
                            {"@context" default-context
                             "from"     ledger-name
                             "select"   {"?t" ["*"]}
                             "where"    {"@id"  "?t"
                                         "type" "schema:Test"}}
                            (:private auth)))
            query-res (api-post :query
                                {:body    (json/stringify query-req)
                                 :headers json-headers})]
        (is (= 200 (:status query-res)))
        (is (= [{"ex:name" "cred test",
                 "ex:foo"  1,
                 "id"      "ex:cred-test"
                 "type"    "schema:Test"}]
               (-> query-res :body (json/parse false))))))

    (testing "history"
      (let [history-req (<!! (cred/generate
                              {"@context" default-context
                               "from"     ledger-name
                               "history"  "ex:cred-test"
                               "t"        {"from" 1}}
                              (:private auth)))

            history-res (api-post :history
                                  {:body    (json/stringify history-req)
                                   :headers json-headers})]
        (is (= 200 (:status history-res)))
        (is (= [{"f:retract" [],
                 "f:assert"
                 [{"ex:name" "cred test",
                   "ex:foo"  1,
                   "id"      "ex:cred-test",
                   "type"    "schema:Test"}],
                 "f:t"       2}]
               (-> history-res :body (json/parse false))))))

    (testing "invalid credential"
      (let [valid-tx    (<!!
                         (cred/generate {"ledger"   ledger-name
                                         "@context" ["https://ns.flur.ee", {"ex" "http://example.com/ns/"}]
                                         "insert"   {"@id"    "ex:cred-test"
                                                     "ex:KEY" "VALUE"}}
                                        (:private auth)))

            invalid-tx  (assoc-in valid-tx ["credentialSubject" "insert" "ex:KEY"]
                                  "ALTEREDVALUE")

            invalid-res (api-post :transact
                                  {:body    (json/stringify invalid-tx)
                                   :headers json-headers})]
        (is (= 400 (:status invalid-res)))
        (is (= {"error" "Invalid credential"}
               (-> invalid-res :body (json/parse false))))))))
