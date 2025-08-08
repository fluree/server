(ns fluree.server.integration.cors-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [fluree.db.util.json :as json]
            [fluree.server.integration.test-system
             :refer [api-post json-headers run-test-server]]))

(use-fixtures :once run-test-server)

(deftest ^:integration test-cors-in-create-endpoint
  (testing "CORS headers are present in create endpoint response"
    (let [ledger-name (str "cors-test-" (random-uuid))
          req         (json/stringify
                       {"ledger" ledger-name
                        "insert" {"@id" "ex:test"
                                  "ex:value" "test"}})
          res         (api-post :create {:body    req
                                         :headers (assoc json-headers
                                                         "Origin" "http://fluree.pi")})]
      (is (= 201 (:status res)))
      ; Check that CORS headers are present in response
      (is (contains? (:headers res) "access-control-allow-origin"))
      (is (contains? (:headers res) "access-control-allow-methods")))))

(deftest ^:integration test-cors-in-options-request
  (testing "OPTIONS request returns proper CORS headers"
    ; Note: This might not work with the test system setup, 
    ; but demonstrates what should be tested
    (let [res (api-post :query {:method  :options
                                :headers {"Origin" "http://fluree.pi"
                                          "Access-Control-Request-Method" "POST"}})]
      ; OPTIONS should return 200 or 204 with CORS headers
      (is (or (= 200 (:status res)) (= 204 (:status res))))
      (is (contains? (:headers res) "access-control-allow-origin")))))