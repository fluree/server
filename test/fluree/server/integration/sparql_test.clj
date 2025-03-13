(ns fluree.server.integration.sparql-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [fluree.server.integration.test-system
             :refer [api-post create-rand-ledger json-headers run-test-server
                     sparql-headers sparql-results-headers]]
            [jsonista.core :as json]))

(use-fixtures :once run-test-server)

(deftest ^:integration ^:sparql query-sparql-test
  (testing "basic SPARQL query works"
    (let [ledger-name (create-rand-ledger "query-sparql-test")
          txn         {"ledger"   ledger-name
                       "@context" {"schema" "http://schema.org/"
                                   "ex"     "http://example.org/"}
                       "insert"   [{"@id"     "ex:query-sparql-test"
                                    "@type"   "schema:Test"
                                    "ex:name" "query-sparql-test"}]}
          txn-res     (api-post :transact {:body    (json/write-value-as-string txn)
                                           :headers json-headers})
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
                                                        "Fluree-Meta" true)})

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
             (-> query-res :body json/read-value)))

      (is (= 200 (:status meta-res)))
      (is (= {"fuel" 2,
              "status" 200,
              "result"
              {"results"
               {"bindings"
                [{"name" {"value" "query-sparql-test", "type" "literal"}}]},
               "head" {"vars" ["name"]}}}
             (-> meta-res :body json/read-value (dissoc "time"))))
      (is (= 200 (:status rdf-res)))
      (is (= {"@context" {"ex" "http://example.org/" "schema" "http://schema.org/"},
              "@graph" [{"@id" "ex:query-sparql-test"
                         "foo:name" ["query-sparql-test"]}]}
             (-> rdf-res :body json/read-value))))))
