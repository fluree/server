(ns fluree.server.integration.closed-mode-test
  (:require [clojure.test :as test :refer [deftest testing is]]
            [fluree.crypto :as crypto]
            [fluree.server.integration.test-system
             :refer [api-post auth json-headers jwt-headers run-closed-test-server]]
            [jsonista.core :as json]))

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
  {:id "did:fluree:Tf4KTeKpWcZAJadKfJ4JUv84dkBYy5KFHod"
   :private "8d542edcd3a11b4ca5faabe7c9fa09045d6f489b9461518dbd86c6c9e3b21fec",
   :public "03ad2f0920fd7e8b77b422f0922f53abd260336be2a3fccdc1bfadd8d858da149b"})

(deftest closed-mode
  ;; all endpoints
  (let [ledger1 (str "closed-test-" (random-uuid))
        ledger2 (str "closed-test-" (random-uuid))]
    (testing "root request"
      (testing "to create"
        (let [create-req {"@context" default-context
                          "ledger"   ledger1
                          "insert"   {"@graph"
                                      [{"id"            (:id root-auth)
                                        "f:policyClass" {"id" "ex:RootPolicy"}
                                        "type"          "schema:Person"
                                        "ex:name"       "Goose"}
                                       {"@id"      "ex:defaultAllowView"
                                        "@type"    ["f:AccessPolicy" "ex:RootPolicy"]
                                        "f:action" [{"@id" "f:view"} {"@id" "f:modify"}]
                                        "f:query"  {"@type"  "@json"
                                                    "@value" {}}}]}}
              resp       (api-post :create {:body    (crypto/create-jws (json/write-value-as-string create-req)
                                                                        (:private root-auth))
                                            :headers jwt-headers})]
          (testing "is accepted"
            (is (= 201 (:status resp))))))
      (testing "to transact"
        (let [transact-req {"ledger"   ledger1
                            "@context" ["https://ns.flur.ee" default-context]
                            "insert"   [{"@id" "ex:coin" "ex:name" "nickel"}]}
              resp         (api-post :transact {:body    (crypto/create-jws (json/write-value-as-string transact-req)
                                                                            (:private root-auth))
                                                :headers jwt-headers})]
          (testing "is accepted"
            (is (= 200 (:status resp))))))
      (testing "to query"
        (let [query-req {"from"     ledger1
                         "@context" default-context
                         "where"    [{"@id" "?s" "ex:name" "?name"}]
                         "select"   "?s"}
              resp      (api-post :query {:body    (crypto/create-jws (json/write-value-as-string query-req)
                                                                      (:private root-auth))
                                          :headers jwt-headers})]
          (testing "is accepted"
            (is (= 200 (:status resp)))
            (is (= ["did:fluree:TfHgFTQQiJMHaK1r1qxVPZ3Ridj9pCozqnh" "ex:coin"]
                   (-> resp :body json/read-value))))))
      (testing "to query history"
        (let [history-req {"from"     ledger1
                           "@context" default-context
                           "history"  "ex:coin"
                           "t"        {"from" 1 "to" "latest"}}
              resp        (api-post :history {:body    (crypto/create-jws (json/write-value-as-string history-req)
                                                                          (:private root-auth))
                                              :headers jwt-headers})]
          (testing "is accepted"
            (is (= 200 (:status resp)))
            (is (= [{"f:retract" [],
                     "f:assert"  [{"ex:name" "nickel", "id" "ex:coin"}],
                     "f:t"       2}]
                   (-> resp :body json/read-value)))))))

    (testing "non-root request"
      (testing "to create"
        (let [create-req {"ledger"   ledger2
                          "@context" ["https://ns.flur.ee" default-context]
                          "insert"   {"@graph"
                                      [{"id"            (:id root-auth)
                                        "f:policyClass" {"id" "ex:RootPolicy"}
                                        "type"          "schema:Person"
                                        "ex:name"       "Root User"}
                                       {"@id"      "ex:defaultAllowView"
                                        "@type"    ["f:AccessPolicy" "ex:RootPolicy"]
                                        "f:action" [{"@id" "f:view"} {"@id" "f:modify"}]
                                        "f:query"  {"@type"  "@json"
                                                    "@value" {}}}]}}
              resp       (api-post :create {:body    (crypto/create-jws (json/write-value-as-string create-req)
                                                                        (:private non-root-auth))
                                            :headers jwt-headers})]
          (testing "is rejected"
            (is (= 403 (:status resp)))
            (is (= {"error" "Untrusted credential."} (-> resp :body json/read-value))))))
      ;; Disabling this test since it seems to conflict with recent policy changes
      ;; (testing "to transact"
      ;;   (let [txn-req {"ledger"   ledger1
      ;;                  "@context" ["https://ns.flur.ee" default-context]
      ;;                  "insert"   [{"@id" "ex:coin" "ex:name" "nickel"}]}
      ;;         resp    (api-post :transact {:body    (crypto/create-jws (json/write-value-as-string txn-req)
      ;;                                                                  (:private non-root-auth))
      ;;                                      :headers jwt-headers})]
      ;;     (testing "is accepted"
      ;;       (is (= 200 (:status resp))))))
      (testing "to query"
        (let [query-req {"from"     ledger1
                         "@context" default-context
                         "where"    [{"@id" "?s" "ex:name" "?name"}]
                         "select"   ["?s" "?name"]}
              resp      (api-post :query {:body    (crypto/create-jws (json/write-value-as-string query-req)
                                                                      (:private non-root-auth))
                                          :headers jwt-headers})]
          (testing "is accepted"
            (is (= 200 (:status resp)))
            (is (= [] (-> resp :body json/read-value))))))
      (testing "to query history"
        (let [query-history-req {"@context" default-context
                                 "from"     ledger1
                                 "history"  "ex:coin"
                                 "t"        {"from" 1}}
              resp              (api-post :history {:body    (crypto/create-jws (json/write-value-as-string query-history-req)
                                                                                (:private non-root-auth))
                                                    :headers jwt-headers})]
          (testing "is accepted"
            (is (= 200 (:status resp)))
            (is (= [{"f:retract" [],
	             "f:assert"  [{"ex:name" "nickel", "id" "ex:coin"}],
	             "f:t"       2}]
                   (-> resp :body json/read-value))))))
      (testing "to claim more authority"
        (let [query-req {"from"     ledger1
                         "@context" default-context
                         "where"    [{"@id" "?s" "ex:name" "?name"}]
                         "select"   ["?s" "?name"]
                         ;; claiming root-auth identity in opts
                         "opts"     {"did" (:id root-auth)}}
              resp      (api-post :query {:body    (crypto/create-jws (json/write-value-as-string query-req)
                                                                      (:private non-root-auth))
                                          :headers jwt-headers})]
          (testing "is silently demoted"
            (is (= 200 (:status resp)))
            (is (= [] (-> resp :body json/read-value)))))))

    (testing "unsigned request"
      (testing "to create"
        (let [create-req {"ledger"   "closed-test3"
                          "@context" ["https://ns.flur.ee" default-context]
                          "insert"   {"@graph"
                                      [{"id"            (:id auth)
                                        "f:policyClass" {"id" "ex:RootPolicy"}
                                        "type"          "schema:Person"
                                        "ex:name"       "Root User"}
                                       {"@id"      "ex:defaultAllowView"
                                        "@type"    ["f:AccessPolicy" "ex:RootPolicy"]
                                        "f:action" [{"@id" "f:view"} {"@id" "f:modify"}]
                                        "f:query"  {"@type"  "@json"
                                                    "@value" {}}}]}}
              resp       (api-post :create {:body    (json/write-value-as-string create-req)
                                            :headers json-headers})]
          (testing "is rejected"
            (is (= 400 (:status resp)))
            (is (= {"error" "Missing credential."} (-> resp :body json/read-value))))))
      (testing "to transact"
        (let [transact-req {"ledger"   "closed-test3"
                            "@context" ["https://ns.flur.ee" default-context]
                            "insert"   [{"@id" "ex:coin" "ex:name" "nickel"}]}
              resp         (api-post :transact {:body    (json/write-value-as-string transact-req)
                                                :headers json-headers})]
          (testing "is rejected"
            (is (= 400 (:status resp)))
            (is (= {"error" "Missing credential."} (-> resp :body json/read-value))))))
      (testing "to query"
        (let [query-req {"from"     "closed-test3"
                         "@context" default-context
                         "where"    [{"@id" "?s" "ex:name" "?name"}]
                         "select"   ["?s" "?name"]}
              resp      (api-post :query {:body    (json/write-value-as-string query-req)
                                          :headers json-headers})]
          (testing "is accepted"
            (is (= 400 (:status resp)))
            (is (= {"error" "Missing credential."} (-> resp :body json/read-value))))))
      (testing "to query history"
        (let [history-req {"from"     "closed-test3"
                           "@context" default-context
                           "commit"   true
                           "t"        {"from" 1 "to" "latest"}}
              resp        (api-post :history {:body    (json/write-value-as-string history-req)
                                              :headers json-headers})]
          (testing "is rejected"
            (is (= 400 (:status resp)))
            (is (= {"error" "Missing credential."} (-> resp :body json/read-value)))))))))
