(ns fluree.server.integration.sparql-test
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is testing use-fixtures]]
            [fluree.db.util.json :as json]
            [fluree.server.integration.test-system :as test-system
             :refer [api-post create-rand-ledger run-test-server sparql-headers
                     sparql-results-headers sparql-update-headers]]))

(use-fixtures :once run-test-server)

(deftest ^:integration ^:sparql query-sparql-test
  (testing "basic SPARQL query works"
    (let [ledger-name (create-rand-ledger "query-sparql-test")
          txn         (str/join "\n"
                                ["PREFIX schema: <http://schema.org/>"
                                 "PREFIX ex: <http://example.org/>"
                                 "INSERT DATA"
                                 (str " { GRAPH <" ledger-name ">")
                                 "{"
                                 "ex:query-sparql-test a schema:Test ; ex:name \"query-sparql-test\" ."
                                 "}"
                                 "}"])
          txn-res     (api-post :transact {:body txn :headers sparql-update-headers})
          _           (assert (= 200 (:status txn-res)))
          query       (str "PREFIX schema: <http://schema.org/>
                                      PREFIX ex: <http://example.org/>
                                      SELECT ?name
                                      FROM   <" ledger-name ">
                                      WHERE  {?test a schema:Test;
                                                    ex:name ?name.}")
          query-res   (api-post :query {:body    query
                                        :headers sparql-results-headers})

          meta-res    (api-post :query {:body    query
                                        :headers (assoc sparql-results-headers
                                                        "Fluree-Track-Meta" true)})

          construct   (str "PREFIX ex: <http://example.org/>
                            PREFIX schema: <http://schema.org/>
                            CONSTRUCT {?test foo:name ?name}
                            FROM <" ledger-name ">
                            WHERE {?test a schema:Test;
                                         ex:name ?name.}")
          rdf-res     (api-post :query {:body construct
                                        :headers sparql-headers})]

      (is (= 200 (:status query-res)))
      (is (= {"head" {"vars" ["name"]}
              "results"
              {"bindings"
               [{"name" {"value" "query-sparql-test", "type" "literal"}}]}}
             (-> query-res :body (json/parse false))))

      (is (= 200 (:status meta-res)))
      (is (= {"results"
              {"bindings"
               [{"name" {"value" "query-sparql-test", "type" "literal"}}]},
              "head" {"vars" ["name"]}}
             (-> meta-res :body (json/parse false))))
      (is (= 2 (-> meta-res :headers (get "x-fdb-fuel") Integer/parseInt)))
      (is (= 200 (:status rdf-res)))
      (is (= {"@context" {"ex" "http://example.org/" "schema" "http://schema.org/"},
              "@graph" [{"@id" "ex:query-sparql-test"
                         "foo:name" ["query-sparql-test"]}]}
             (-> rdf-res :body (json/parse false)))))))

(deftest sparql-federated-query
  (let [ledger-name "test-federated-query"
        service     (str "http://localhost:" @test-system/api-port "/fluree/ledger/" ledger-name "/query")

        txn-req     {"ledger" ledger-name
                     "@context" {"ex" "http://example.com/"}
                     "insert" (mapv #(do {"@id" (str "ex:" %)
                                          "@type" (if (odd? %) "ex:Odd" "ex:Even")
                                          "ex:name" (str "name" %)
                                          "ex:num" [(dec %) % (inc %)]
                                          "ex:ref" {"@id" (str "ex:" (inc %))}})
                                    (range 10))}
        txn-res (api-post :create {:body (json/stringify txn-req) :headers test-system/json-headers})

        service-q (str/join "\n" ["PREFIX ex: <http://example.com/>"
                                  "SELECT ?name ?type"
                                  (str "FROM <" ledger-name ">")
                                  "WHERE { ?s a ex:Even ; ex:ref ?ref . "
                                  (str "SERVICE <" service "> { ?ref ex:name ?name ; a ?type . }")
                                  "}"])
        service-res (api-post :query {:body service-q :headers sparql-headers})]
    (is (= 201 (:status txn-res)))
    (is (= 200 (:status service-res)))
    (testing "federated query can join across local and remote graphs"
      (is (= [["name1" "ex:Odd"]
              ["name3" "ex:Odd"]
              ["name5" "ex:Odd"]
              ["name7" "ex:Odd"]
              ["name9" "ex:Odd"]]
             (-> service-res :body (json/parse false)))))))
