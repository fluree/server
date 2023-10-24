(ns fluree.server.integration.credential-test
  (:require [clojure.core.async :refer [<!!]]
            [clojure.test :as test :refer [deftest testing is]]
            [fluree.db.json-ld.credential :as cred]
            [fluree.server.integration.test-system :refer :all]
            [jsonista.core :as json]))

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
      ;; cannot transact without roles already defined
      (let [create-req {"f:ledger" ledger-name
                        "@context" default-context
                        "graph"    [{"id"      (:id auth)
                                     "f:role"  {"id" "role:root"}
                                     "type"    "schema:Person"
                                     "ex:name" "Goose"}
                                    {"id"           "ex:rootPolicy"
                                     "type"         "f:Policy"
                                     "f:targetNode" {"id" "f:allNodes"}
                                     "f:allow"
                                     [{"f:targetRole" {"id" "role:root"}
                                       "f:action"     [{"id" "f:view"}
                                                       {"id" "f:modify"}]}]}]}
            create-res (api-post :create
                                 {:body    (json/write-value-as-string create-req)
                                  :headers json-headers})]
        (is (= 201 (:status create-res)))
        (is (= {"ledger" "credential-test"
                "t"      1}
               (-> create-res :body json/read-value (select-keys ["ledger" "t"]))))))
    (testing "transact"
      (let [txn-req (<!! (cred/generate
                          {"ledger" ledger-name
                           "@context" "https://ns.flur.ee"
                           "insert"   [{"id"      "ex:cred-test"
                                        "type"    "schema:Test"
                                        "ex:name" "cred test"
                                        "ex:foo"  1}]}
                          (:private auth)))
            txn-res (api-post :transact
                              {:body    (json/write-value-as-string txn-req)
                               :headers json-headers})]
        (is (= 200 (:status txn-res)))
        (is (= {"ledger" "credential-test"
                "t"      2}
               (-> txn-res :body json/read-value (select-keys ["ledger" "t"]))))))
    (testing "query"
      (let [query-req (<!! (cred/generate
                            {"from"   ledger-name
                             "select" {"?t" ["*"]}
                             "where"  [["?t" "type" "schema:Test"]]}
                            (:private auth)))
            query-res (api-post :query
                                {:body    (json/write-value-as-string query-req)
                                 :headers json-headers})]
        (is (= 200 (:status query-res)))
        (is (= [{"ex:name" "cred test",
                 "ex:foo"  1,
                 "id"      "ex:cred-test"
                 "type"    "schema:Test"}]
               (-> query-res :body json/read-value)))))

    (testing "history"
      (let [history-req (<!! (cred/generate
                              {"from"    ledger-name
                               "history" "ex:cred-test"
                               "t"       {"from" 1}}
                              (:private auth)))

            history-res (api-post :history
                                  {:body    (json/write-value-as-string history-req)
                                   :headers json-headers})]
        (is (= 200 (:status history-res)))
        (is (= [{"f:retract" [],
                 "f:assert"
                 [{"ex:name" "cred test",
                   "ex:foo"  1,
                   "id"      "ex:cred-test",
                   "type"    "schema:Test"}],
                 "f:t"       2}]
               (-> history-res :body json/read-value)))))

    (testing "invalid credential"
      (let [invalid-tx  (-> {"ledger" "credential-test"
                             "@context" "https://ns.flur.ee"
                             "insert"   {"@id"    "ex:cred-test"
                                         "ex:KEY" "VALUE"}}
                            (cred/generate (:private auth))
                            <!!
                            (assoc-in ["credentialSubject" "@graph" "ex:KEY"]
                                      "ALTEREDVALUE"))

            invalid-res (api-post :transact
                                  {:body    (json/write-value-as-string invalid-tx)
                                   :headers json-headers})]
        (is (= 400 (:status invalid-res)))
        (is (= {"error" "Invalid credential"}
               (-> invalid-res :body json/read-value)))))))
