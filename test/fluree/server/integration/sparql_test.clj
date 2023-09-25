(ns fluree.server.integration.sparql-test
  (:require [clojure.test :refer :all]
            [fluree.server.integration.test-system :refer :all]
            [jsonista.core :as json]))

(use-fixtures :once run-test-server)

(deftest ^:integration ^:sparql query-sparql-test
  (testing "basic SPARQL query works"
    (let [ledger-name (create-rand-ledger "query-sparql-test")
          txn-req     {:body
                       (json/write-value-as-string
                        {"f:ledger" ledger-name
                         "@graph"   [{"id"      "ex:query-sparql-test"
                                      "type"    "schema:Test"
                                      "ex:name" "query-sparql-test"}]})
                       :headers json-headers}
          txn-res     (api-post :transact txn-req)
          _           (assert (= 200 (:status txn-res)))
          query-req   {:body    (str "SELECT ?name
                                      FROM   <" ledger-name ">
                                      WHERE  {?test <type> schema:Test;
                                                    ex:name ?name.}")
                       :headers sparql-headers}
          query-res   (api-post :query query-req)]
      (is (= 200 (:status query-res)))
      (is (= [["query-sparql-test"]]
             (-> query-res :body json/read-value))))))
