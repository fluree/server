(ns fluree.server.integration.closed-mode-test
  (:require [clojure.test :as test :refer [deftest testing is]]
            [fluree.crypto :as crypto]
            [fluree.db.util.json :as json]
            [fluree.server.integration.test-system
             :refer [api-post auth json-headers jwt-headers run-closed-test-server]]))

(test/use-fixtures :once run-closed-test-server)

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

(def root-auth auth)
(def non-root-auth
  {:id "did:key:z6MkiKJFxJJd9QuqKgzBR2kiSybE9V2517sFd2kTS7kQe9mg",
   :public "39649a89208b4fbcf818de6716b17a0b1a2f7b63ad7de55187130f39a4ef7157",
   :private "8d542edcd3a11b4ca5faabe7c9fa09045d6f489b9461518dbd86c6c9e3b21fec"})

(deftest closed-mode
  ;; all endpoints
  (testing "root request"
    (testing "to create"
      (let [create-req {"@context" default-context
                        "ledger" "closed-test"
                        "insert" {"@graph"
                                  [{"id" (:id root-auth)
                                    "f:policyClass" {"id" "ex:RootPolicy"}
                                    "type" "schema:Person"
                                    "ex:name" "Root User"}
                                   {"@id" "ex:defaultAllowView"
                                    "@type" ["f:AccessPolicy" "ex:RootPolicy"]
                                    "f:action" [{"@id" "f:view"} {"@id" "f:modify"}]
                                    "f:query" {"@type" "@json"
                                               "@value" {}}}]}}
            resp (api-post :create {:body    (crypto/create-jws (json/stringify create-req)
                                                                (:private root-auth)
                                                                {:include-pubkey true})
                                    :headers jwt-headers})]
        (testing "is accepted"
          (is (= 201 (:status resp))))))
    (testing "to transact"
      (let [transact-req {"ledger" "closed-test"
                          "@context" default-context
                          "insert" [{"@id" "ex:coin" "ex:name" "nickel"}]}
            resp (api-post :transact {:body (crypto/create-jws (json/stringify transact-req)
                                                               (:private root-auth)
                                                               {:include-pubkey true})
                                      :headers jwt-headers})]
        (testing "is accepted"
          (is (= 200 (:status resp))))))
    (testing "to query"
      (let [query-req {"from" "closed-test"
                       "@context" default-context
                       "where" [{"@id" "?s" "ex:name" "?name"}]
                       "select" "?s"}
            resp (api-post :query {:body (crypto/create-jws (json/stringify query-req)
                                                            (:private root-auth)
                                                            {:include-pubkey true})
                                   :headers jwt-headers})]
        (testing "is accepted"
          (is (= 200 (:status resp)))
          (is (= ["did:key:z6MkmbNqfM3ANYZnzDp9YDfa62pHggKosBkCyVdgQtgEKkGQ" "ex:coin"]
                 (-> resp :body (json/parse false)))))))
    (testing "to query history"
      (let [history-req {"from" "closed-test"
                         "@context" default-context
                         "history" "ex:coin"
                         "t" {"from" 1 "to" "latest"}}
            resp (api-post :history {:body (crypto/create-jws (json/stringify history-req)
                                                              (:private root-auth)
                                                              {:include-pubkey true})
                                     :headers jwt-headers})]
        (testing "is accepted"
          (is (= 200 (:status resp)))
          (is (= [{"f:retract" [],
                   "f:assert" [{"ex:name" "nickel", "id" "ex:coin"}],
                   "f:t" 2}]
                 (-> resp :body (json/parse false)))))))
    (testing "to drop"
      (let [drop-req {"ledger" "closed-test"}
            resp     (api-post :drop {:body (crypto/create-jws (json/stringify drop-req)
                                                               (:private root-auth)
                                                               {:include-pubkey true})
                                      :headers jwt-headers})]
        (testing "is accepted"
          (is (= 200 (:status resp)))))))

  (testing "non-root request"
    (testing "to create"
      (let [create-req {"ledger" "closed-test2"
                        "@context" default-context
                        "insert" {"@graph"
                                  [{"id" (:id root-auth)
                                    "f:policyClass" {"id" "ex:RootPolicy"}
                                    "type" "schema:Person"
                                    "ex:name" "Root User"}
                                   {"@id" "ex:defaultAllowView"
                                    "@type" ["f:AccessPolicy" "ex:RootPolicy"]
                                    "f:action" [{"@id" "f:view"} {"@id" "f:modify"}]
                                    "f:query" {"@type" "@json"
                                               "@value" {}}}]}}
            resp (api-post :create {:body    (crypto/create-jws (json/stringify create-req)
                                                                (:private non-root-auth)
                                                                {:include-pubkey true})
                                    :headers jwt-headers})]
        (testing "is rejected"
          (is (= 403 (:status resp)))
          (is (= {"error" "Untrusted credential."} (-> resp :body (json/parse false)))))

        ;; create as root for further testing
        (api-post :create {:body (crypto/create-jws (json/stringify create-req)
                                                    (:private root-auth)
                                                    {:include-pubkey true})
                           :headers jwt-headers})))
    (testing "to transact"
      (let [create-req {"ledger" "closed-test2"
                        "@context" default-context
                        "insert" [{"@id" "ex:coin" "ex:name" "nickel"}]}
            resp (api-post :transact {:body (crypto/create-jws (json/stringify create-req)
                                                               (:private non-root-auth)
                                                               {:include-pubkey true})
                                      :headers jwt-headers})]
        (testing "is rejected"
          (is (= 403 (:status resp))))))
    (testing "to query"
      (let [create-req {"from" "closed-test2"
                        "@context" default-context
                        "where" [{"@id" "?s" "ex:name" "?name"}]
                        "select" ["?s" "?name"]}
            resp (api-post :query {:body (crypto/create-jws (json/stringify create-req)
                                                            (:private non-root-auth)
                                                            {:include-pubkey true})
                                   :headers jwt-headers})]
        (testing "is accepted"
          (is (= 200 (:status resp)))
          (is (= [] (-> resp :body (json/parse false)))))))
    (testing "to query history"
      (let [create-req {"@context" default-context
                        "from" "closed-test2"
                        "history" "ex:coin"
                        "t" {"from" 1}}
            resp (api-post :history {:body (crypto/create-jws (json/stringify create-req)
                                                              (:private non-root-auth)
                                                              {:include-pubkey true})
                                     :headers jwt-headers})]
        (testing "is accepted"
          (is (= 200 (:status resp)))
          (is (= [] (-> resp :body (json/parse false)))))))
    (testing "to claim more authority"
      (let [create-req {"from" "closed-test2"
                        "@context" default-context
                        "where" [{"@id" "?s" "ex:name" "?name"}]
                        "select" ["?s" "?name"]
                        ;; claiming root-auth identity in opts
                        "opts" {"did" (:id root-auth)}}
            resp (api-post :query {:body (crypto/create-jws (json/stringify create-req)
                                                            (:private non-root-auth)
                                                            {:include-pubkey true})
                                   :headers jwt-headers})]
        (testing "is silently demoted"
          (is (= 200 (:status resp)))
          (is (= [] (-> resp :body (json/parse false)))))))
    (testing "to drop"
      (let [drop-req {"ledger" "closed-test2"}
            resp     (api-post :drop {:body (crypto/create-jws (json/stringify drop-req)
                                                               (:private non-root-auth)
                                                               {:include-pubkey true})
                                      :headers jwt-headers})]
        (testing "is rejected"
          (is (= 403 (:status resp)))))))

  (testing "unsigned request"
    (testing "to create"
      (let [create-req {"ledger" "closed-test3"
                        "@context" default-context
                        "insert" {"@graph"
                                  [{"id" (:id auth)
                                    "f:policyClass" {"id" "ex:RootPolicy"}
                                    "type" "schema:Person"
                                    "ex:name" "Root User"}
                                   {"@id" "ex:defaultAllowView"
                                    "@type" ["f:AccessPolicy" "ex:RootPolicy"]
                                    "f:action" [{"@id" "f:view"} {"@id" "f:modify"}]
                                    "f:query" {"@type" "@json"
                                               "@value" {}}}]}}
            resp (api-post :create {:body    (json/stringify create-req)
                                    :headers json-headers})]
        (testing "is rejected"
          (is (= 400 (:status resp)))
          (is (= {"error" "Missing credential."} (-> resp :body (json/parse false)))))
        ;; create as root for further testing
        (api-post :create {:body (crypto/create-jws (json/stringify create-req)
                                                    (:private root-auth))
                           :headers jwt-headers})))
    (testing "to transact"
      (let [transact-req {"ledger" "closed-test3"
                          "@context" default-context
                          "insert" [{"@id" "ex:coin" "ex:name" "nickel"}]}
            resp (api-post :transact {:body (json/stringify transact-req)
                                      :headers json-headers})]
        (testing "is rejected"
          (is (= 400 (:status resp)))
          (is (= {"error" "Missing credential."} (-> resp :body (json/parse false)))))))
    (testing "to query"
      (let [query-req {"from" "closed-test3"
                       "@context" default-context
                       "where" [{"@id" "?s" "ex:name" "?name"}]
                       "select" ["?s" "?name"]}
            resp (api-post :query {:body (json/stringify query-req)
                                   :headers json-headers})]
        (testing "is accepted"
          (is (= 400 (:status resp)))
          (is (= {"error" "Missing credential."} (-> resp :body (json/parse false)))))))
    (testing "to query history"
      (let [history-req {"from" "closed-test3"
                         "@context" default-context
                         "commit" true
                         "t" {"from" 1 "to" "latest"}}
            resp (api-post :history {:body (json/stringify history-req)
                                     :headers json-headers})]
        (testing "is rejected"
          (is (= 400 (:status resp)))
          (is (= {"error" "Missing credential."} (-> resp :body (json/parse false)))))))
    (testing "to drop"
      (let [drop-req {"ledger" "closed-test3"}
            resp     (api-post :drop {:body (json/stringify drop-req)
                                      :headers json-headers})]
        (testing "is rejected"
          (is (= 400 (:status resp)))
          (is (= {"error" "Missing credential."} (-> resp :body (json/parse false)))))))))
