(ns fluree.server.integration.basic-query-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [fluree.server.integration.test-system
             :as test-system
             :refer [api-post create-rand-ledger json-headers run-test-server]]
            [jsonista.core :as json]))

(use-fixtures :once run-test-server)

(deftest ^:integration ^:json query-json-test
  (testing "can query a basic entity w/ JSON"
    (let [ledger-name (create-rand-ledger "query-endpoint-basic-entity-test")
          txn-req     {:body
                       (json/write-value-as-string
                        {"ledger"   ledger-name
                         "@context" test-system/default-context
                         "insert"   [{"id"      "ex:query-test"
                                      "type"    "schema:Test"
                                      "ex:name" "query-test"}]})
                       :headers json-headers}
          txn-res     (api-post :transact txn-req)
          _           (assert (= 200 (:status txn-res)))
          query-req   {:body
                       (json/write-value-as-string
                        {"@context" test-system/default-context
                         "from"     ledger-name
                         "select"   '{?t ["*"]}
                         "where"    '{"id" ?t, "type" "schema:Test"}})
                       :headers json-headers}
          query-res   (api-post :query query-req)]
      (is (= 200 (:status query-res)))
      (is (= [{"id"      "ex:query-test"
               "type"    "schema:Test"
               "ex:name" "query-test"}]
             (-> query-res :body json/read-value)))))

  (testing "union query works"
    (let [ledger-name (create-rand-ledger "query-endpoint-union-test")
          txn-req     {:body
                       (json/write-value-as-string
                        {"ledger"   ledger-name
                         "@context" test-system/default-context
                         "insert"   {"@graph"
                                     [{"id"      "ex:query-test"
                                       "type"    "schema:Test"
                                       "ex:name" "query-test"}
                                      {"id"       "ex:wes"
                                       "type"     "schema:Person"
                                       "ex:fname" "Wes"}]}})
                       :headers json-headers}
          txn-res     (api-post :transact txn-req)
          _           (assert (= 200 (:status txn-res)))
          query-req   {:body
                       (json/write-value-as-string
                        {"@context" test-system/default-context
                         "from"     ledger-name
                         "select"   "?n"
                         "where"    [["union"
                                      {"id" "?s", "ex:name" "?n"}
                                      {"id" "?s", "ex:fname" "?n"}]]})
                       :headers json-headers}
          query-res   (api-post :query query-req)]
      (is (= 200 (:status query-res)))
      (is (= ["query-test" "Wes"]
             (-> query-res :body json/read-value)))))

  (testing "optional query works"
    (let [ledger-name (create-rand-ledger "query-endpoint-optional-test")
          txn-req     {:body
                       (json/write-value-as-string
                        {"ledger"   ledger-name
                         "@context" test-system/default-context
                         "insert"   {"@graph"
                                     [{"id"          "ex:brian",
                                       "type"        "ex:User",
                                       "schema:name" "Brian"
                                       "ex:friend"   [{"id" "ex:alice"}]}
                                      {"id"           "ex:alice",
                                       "type"         "ex:User",
                                       "ex:favColor"  "Green"
                                       "schema:email" "alice@flur.ee"
                                       "schema:name"  "Alice"}
                                      {"id"           "ex:cam",
                                       "type"         "ex:User",
                                       "schema:name"  "Cam"
                                       "schema:email" "cam@flur.ee"
                                       "ex:friend"    [{"id" "ex:brian"}
                                                       {"id" "ex:alice"}]}]}})
                       :headers json-headers}
          txn-res     (api-post :transact txn-req)
          _           (assert (= 200 (:status txn-res)))
          query       {"@context" test-system/default-context
                       "from"     ledger-name
                       "select"   '[?name ?favColor]
                       "where"    '[{"id"          ?s
                                     "rdf:type"    "ex:User"
                                     "schema:name" ?name}
                                    ["optional" {"id" ?s, "ex:favColor" ?favColor}]]}
          query-req   {:body
                       (json/write-value-as-string query)
                       :headers json-headers}
          query-res   (api-post :query query-req)]
      (is (= 200 (:status query-res))
          (str "Response was: " (pr-str query-res)))
      (is (= #{["Cam" nil]
               ["Alice" "Green"]
               ["Brian" nil]}
             (-> query-res :body json/read-value set))
          (str "Response was: " (pr-str query-res)))))

  (testing "selectOne query works"
    (let [ledger-name (create-rand-ledger "query-endpoint-selectOne-test")
          txn-req     {:body
                       (json/write-value-as-string
                        {"ledger"   ledger-name
                         "@context" test-system/default-context
                         "insert"   [{"id"      "ex:query-test"
                                      "type"    "schema:Test"
                                      "ex:name" "query-test"}]})
                       :headers json-headers}
          txn-res     (api-post :transact txn-req)
          _           (assert (= 200 (:status txn-res)))
          query-req   {:body
                       (json/write-value-as-string
                        {"@context"  test-system/default-context
                         "from"      ledger-name
                         "selectOne" '{?t ["*"]}
                         "where"     '{"id" ?t, "type" "schema:Test"}})
                       :headers json-headers}
          query-res   (api-post :query query-req)]
      (is (= 200 (:status query-res)))
      (is (= {"id"      "ex:query-test"
              "type"    "schema:Test"
              "ex:name" "query-test"}
             (-> query-res :body json/read-value)))))

  (testing "bind query works"
    (let [ledger-name (create-rand-ledger "query-endpoint-bind-test")
          context     {"id"     "@id"
                       "type"   "@type"
                       "xsd"    "http://www.w3.org/2001/XMLSchema#"
                       "rdf"    "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                       "rdfs"   "http://www.w3.org/2000/01/rdf-schema#"
                       "sh"     "http://www.w3.org/ns/shacl#"
                       "schema" "http://schema.org/"
                       "skos"   "http://www.w3.org/2008/05/skos#"
                       "wiki"   "https://www.wikidata.org/wiki/"
                       "f"      "https://ns.flur.ee/ledger#"
                       "ex"     "http://example.org/"}
          txn-req     {:headers json-headers
                       :body
                       (json/write-value-as-string
                        {"ledger"   ledger-name
                         "@context" context
                         "insert"
                         {"@graph"
                          [{"id"          "ex:freddy"
                            "type"        "ex:Yeti"
                            "schema:age"  4
                            "schema:name" "Freddy"
                            "ex:verified" true}
                           {"id"             "ex:letty"
                            "type"           "ex:Yeti"
                            "schema:age"     2
                            "ex:nickname"    "Letty"
                            "schema:name"    "Leticia"
                            "schema:follows" [{"id" "ex:freddy"}]}
                           {"id"          "ex:betty"
                            "type"        "ex:Yeti"
                            "schema:age"  82
                            "schema:name" "Betty"
                            "schema:follows"
                            [{"@id" "ex:freddy"}]}
                           {"id"          "ex:andrew"
                            "type"        "schema:Person"
                            "schema:age"  35
                            "schema:name" "Andrew Johnson"
                            "schema:follows"
                            [{"@id" "ex:freddy"}
                             {"@id" "ex:letty"}
                             {"@id" "ex:betty"}]}]}})}

          txn-res   (api-post :transact txn-req)
          _         (assert (= 200 (:status txn-res)))
          query-req {:body
                     (json/write-value-as-string
                      {"@context" context
                       "from"     ledger-name
                       "select"   ["?name" "?age" "?canVote"]
                       "where"    [{"schema:name" "?name"
                                    "schema:age"  "?age"}
                                   ["bind" "?canVote" "(>= ?age 18)"]]
                       "orderBy"  ["?name"]})
                     :headers json-headers}
          query-res (api-post :query query-req)]
      (is (= 200 (:status query-res))
          (str "Query response was: " (pr-str query-res)))
      (is (= [["Andrew Johnson" 35 true]
              ["Betty" 82 true]
              ["Freddy" 4 false]
              ["Leticia" 2 false]]
             (-> query-res :body json/read-value))))))

#_(deftest ^:integration ^:edn query-edn-test
    (testing "can query a basic entity w/ EDN"
      (let [ledger-name (create-rand-ledger "query-endpoint-basic-entity-test")
            edn-context {:id     "@id"
                         :type   "@type"
                         :ex     "http://example.org/"
                         :schema "http://schema.org/"}
            txn-req     {:body
                         (pr-str {:ledger ledger-name
                                  :txn    {"@context" edn-context
                                           "@graph"   [{:id      :ex/query-test
                                                        :type    :schema/Test
                                                        :ex/name "query-test"}]}})
                         :headers edn-headers}
            txn-res     (post :transact txn-req)
            _           (assert (= 200 (:status txn-res)))
            query-req   {:body
                         (pr-str {:context edn-context
                                  :from    ledger-name
                                  :select  '{?t [:*]}
                                  :where   '{:id ?t :type :schema/Test}})
                         :headers edn-headers}
            query-res   (post :query query-req)]
        (is (= 200 (:status query-res))
            (str "Query response was:" (pr-str query-res)))
        (is (= [{:id       :ex/query-test
                 :rdf/type [:schema/Test]
                 :ex/name  "query-test"}]
               (-> query-res :body edn/read-string))))))
