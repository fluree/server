(ns fluree.server.integration.basic-query-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [fluree.db.util.json :as json]
            [fluree.server.integration.test-system
             :as test-system
             :refer [api-post create-rand-ledger json-headers run-test-server]]))

(use-fixtures :once run-test-server)

(deftest ^:integration ^:json query-json-test
  (testing "can query a basic entity w/ JSON"
    (let [ledger-name (create-rand-ledger "query-endpoint-basic-entity-test")
          txn-req     {:body
                       (json/stringify
                        {"ledger"   ledger-name
                         "@context" test-system/default-context
                         "insert"   [{"id"      "ex:query-test"
                                      "type"    "schema:Test"
                                      "ex:name" "query-test"}]})
                       :headers json-headers}
          txn-res     (api-post :transact txn-req)
          _           (assert (= 200 (:status txn-res)))
          query-req   {:body
                       (json/stringify
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
             (-> query-res :body (json/parse false))))))

  (testing "union query works"
    (let [ledger-name (create-rand-ledger "query-endpoint-union-test")
          txn-req     {:body
                       (json/stringify
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
                       (json/stringify
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
             (-> query-res :body (json/parse false))))))

  (testing "optional query works"
    (let [ledger-name (create-rand-ledger "query-endpoint-optional-test")
          txn-req     {:body
                       (json/stringify
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
                       (json/stringify query)
                       :headers json-headers}
          query-res   (api-post :query query-req)]
      (is (= 200 (:status query-res))
          (str "Response was: " (pr-str query-res)))
      (is (= #{["Cam" nil]
               ["Alice" "Green"]
               ["Brian" nil]}
             (-> query-res :body (json/parse false) set))
          (str "Response was: " (pr-str query-res)))))

  (testing "selectOne query works"
    (let [ledger-name (create-rand-ledger "query-endpoint-selectOne-test")
          txn-req     {:body
                       (json/stringify
                        {"ledger"   ledger-name
                         "@context" test-system/default-context
                         "insert"   [{"id"      "ex:query-test"
                                      "type"    "schema:Test"
                                      "ex:name" "query-test"}]})
                       :headers json-headers}
          txn-res     (api-post :transact txn-req)]
      (assert (= 200 (:status txn-res)))
      (testing "with result"
        (let [query-req {:body
                         (json/stringify
                          {"@context" test-system/default-context
                           "from" ledger-name
                           "selectOne" '{?t ["*"]}
                           "where" '{"id" ?t, "type" "schema:Test"}})
                         :headers json-headers}
              query-res (api-post :query query-req)]
          (is (= 200 (:status query-res)))
          (is (= {"id"      "ex:query-test"
                  "type"    "schema:Test"
                  "ex:name" "query-test"}
                 (-> query-res :body (json/parse false))))))
      (testing "without result"
        (let [query-req {:body
                         (json/stringify
                          {"@context" test-system/default-context
                           "from" ledger-name
                           "selectOne" '{?t ["*"]}
                           "where" '{"id" ?t, "type" "schema:Foo"}})
                         :headers json-headers}
              query-res (api-post :query query-req)]
          (is (= 200 (:status query-res)))
          (is (nil? (-> query-res :body (json/parse false))))))))

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
                       (json/stringify
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
                     (json/stringify
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
             (-> query-res :body (json/parse false)))))))

(deftest ledger-specific-request-test
  (let [ledger-name "ledger-endpoint/basic-query-test"]
    (testing "create"
      (let [create-req1 {"ledger"   ledger-name
                         "@context" {"ex" "http://example.com/"}
                         "insert"   (mapv #(do {"@id"     (str "ex:" %)
                                                "@type"   (if (odd? %) "ex:Odd" "ex:Even")
                                                "ex:name" (str "name" %)
                                                "ex:num"  [(dec %) % (inc %)]
                                                "ex:ref"  {"@id" (str "ex:" (inc %))}})
                                          (range 10))}
            create-res1 (api-post :create {:body (json/stringify create-req1) :headers json-headers})

            ledger-name2 (str ledger-name 2)
            create-req2  {"ledger"   ledger-name2
                          "@context" {"ex" "http://example.com/"}
                          "insert"   (mapv #(do {"@id"     (str "ex:" %)
                                                 "@type"   (if (odd? %) "ex:Odd" "ex:Even")
                                                 "ex:name" (str "name" %)
                                                 "ex:num"  [(dec %) % (inc %)]
                                                 "ex:ref"  {"@id" (str "ex:" (inc %))}})
                                           (range 10))}
            create-res2  (api-post (str "/ledger/" ledger-name2 "/:create") {:body (json/stringify create-req2) :headers json-headers})]
        (is (= 201 (:status create-res1)))
        (is (= 201 (:status create-res2)))))
    (testing "transact"
      (let [tx-req {"ledger"   ledger-name
                    "@context" {"ex" "http://example.com/"}
                    "insert"   {"@id" "ex:1" "ex:op" "transact"}}
            tx-res (api-post (str "/ledger/" ledger-name "/:transact") {:body (json/stringify tx-req) :headers json-headers})]
        (is (= 200 (:status tx-res)))))
    (testing "update"
      (let [tx-req {"ledger"   ledger-name
                    "@context" {"ex" "http://example.com/"}
                    "insert"   {"@id" "ex:1" "ex:op" "update"}}
            tx-res (api-post (str "/ledger/" ledger-name "/:update") {:body (json/stringify tx-req) :headers json-headers})]
        (is (= 200 (:status tx-res)))))
    (testing "upsert"
      (let [tx-req {"@context" {"ex" "http://example.com/"}
                    "@id"      "ex:1" "ex:op2" "upsert"}
            tx-res (api-post (str "/ledger/" ledger-name "/:upsert") {:body (json/stringify tx-req) :headers json-headers})]
        (is (= 200 (:status tx-res)))))
    (testing "insert"
      (let [tx-req {"@context" {"ex" "http://example.com/"}
                    "@id"      "ex:1" "ex:op2" "insert"}
            tx-res (api-post (str "/ledger/" ledger-name "/:insert") {:body (json/stringify tx-req) :headers json-headers})]
        (is (= 200 (:status tx-res)))))
    (testing "query"
      (testing "implicit"
        (let [query-req {"@context" {"ex" "http://example.com/"}
                         "select"   {"ex:1" ["ex:op" "ex:op2"]}}
              query-res (api-post (str "ledger/" ledger-name)
                                  {:body (json/stringify query-req) :headers json-headers})]
          (is (= 200 (:status query-res)))
          (is (= [{"ex:op"  ["transact" "update"]
                   "ex:op2" ["insert" "upsert"]}]
                 (-> query-res :body (json/parse false))))))
      (testing "explicit"
        (let [query-req {"@context" {"ex" "http://example.com/"}
                         "select"   {"ex:1" ["ex:op" "ex:op2"]}}
              query-res (api-post (str "ledger/" ledger-name "/:query")
                                  {:body (json/stringify query-req) :headers json-headers})]
          (is (= 200 (:status query-res)))
          (is (= [{"ex:op"  ["transact" "update"]
                   "ex:op2" ["insert" "upsert"]}]
                 (-> query-res :body (json/parse false)))))))
    (testing "history"
      (let [query-req {"@context" {"ex" "http://example.com/"}
                       "from"     ledger-name
                       "history"  "ex:1"
                       "t"        {"from" 2}}
            query-res (api-post (str "ledger/" ledger-name "/:history")
                                {:body (json/stringify query-req) :headers json-headers})]
        (is (= 200 (:status query-res)))
        (is (= [{"https://ns.flur.ee/ledger#t" 2,
                 "https://ns.flur.ee/ledger#assert" [{"@id" "ex:1", "ex:op" "transact"}],
                 "https://ns.flur.ee/ledger#retract" []}
                {"https://ns.flur.ee/ledger#t" 3,
                 "https://ns.flur.ee/ledger#assert" [{"@id" "ex:1", "ex:op" "update"}],
                 "https://ns.flur.ee/ledger#retract" []}
                {"https://ns.flur.ee/ledger#t" 4,
                 "https://ns.flur.ee/ledger#assert" [{"@id" "ex:1", "ex:op2" "upsert"}],
                 "https://ns.flur.ee/ledger#retract" []}
                {"https://ns.flur.ee/ledger#t" 5,
                 "https://ns.flur.ee/ledger#assert" [{"@id" "ex:1", "ex:op2" "insert"}],
                 "https://ns.flur.ee/ledger#retract" []}]
               (-> query-res :body (json/parse false))))))

    (testing "nonmatching routes are handled uniformly"
      (let [not-found1 (api-post "foo" {:body "{}" :headers json-headers})
            not-found2 (api-post (str "ledger/" ledger-name "/:foo") {:body "{}" :headers json-headers})]
        (is (= 404 (:status not-found1)))
        (is (= 404 (:status not-found2)))))))

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
