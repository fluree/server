(ns fluree.server.integration.streaming-query-test
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is testing use-fixtures]]
            [fluree.db.util.json :as json]
            [fluree.server.integration.test-system
             :as test-system
             :refer [api-post create-rand-ledger json-headers run-test-server]]))

(use-fixtures :once run-test-server)

(deftest ^:integration streaming-query-test
  (testing "Query with Accept: application/x-ndjson returns NDJSON stream"
    (let [ledger-name (create-rand-ledger "streaming-query-test")
          ;; Insert test data
          txn-req     {:body
                       (json/stringify
                        {"ledger"   ledger-name
                         "@context" test-system/default-context
                         "insert"   [{"id"      "ex:alice"
                                      "type"    "schema:Person"
                                      "ex:name" "Alice"
                                      "ex:age"  30}
                                     {"id"      "ex:bob"
                                      "type"    "schema:Person"
                                      "ex:name" "Bob"
                                      "ex:age"  25}
                                     {"id"      "ex:charlie"
                                      "type"    "schema:Person"
                                      "ex:name" "Charlie"
                                      "ex:age"  35}]})
                       :headers json-headers}
          txn-res     (api-post :transact txn-req)
          _           (assert (= 200 (:status txn-res)))

          ;; Streaming query
          query-req   {:body
                       (json/stringify
                        {"@context" test-system/default-context
                         "from"     ledger-name
                         "select"   ["?name" "?age"]
                         "where"    {"id"      "?person"
                                     "type"    "schema:Person"
                                     "ex:name" "?name"
                                     "ex:age"  "?age"}})
                       :headers (assoc json-headers "accept" "application/x-ndjson")}
          query-res   (api-post :query query-req)]

      (is (= 206 (:status query-res)))
      (is (= "application/x-ndjson" (get-in query-res [:headers "content-type"])))

      ;; Parse NDJSON response
      (let [lines (-> query-res :body str/split-lines)
            results (map #(json/parse % false) lines)
            ;; Last line is metadata, rest are data
            data-results (butlast results)
            meta-result (last results)]
        ;; Should get 3 data results + 1 metadata result
        (is (= 4 (count results)))
        ;; Data results are vectors (tuples) in streaming mode: [name age]
        (is (every? vector? data-results))
        ;; Metadata result contains :_fluree-meta key
        (is (map? meta-result))
        (is (contains? meta-result "_fluree-meta"))
        (is (= 200 (get-in meta-result ["_fluree-meta" "status"])))
        ;; Verify all three people are in results (first element of each tuple is name)
        (is (= #{"Alice" "Bob" "Charlie"}
               (set (map first data-results)))))))

  (testing "Query without Accept header returns buffered JSON array"
    (let [ledger-name (create-rand-ledger "buffered-query-test")
          txn-req     {:body
                       (json/stringify
                        {"ledger"   ledger-name
                         "@context" test-system/default-context
                         "insert"   {"id"      "ex:test"
                                     "type"    "schema:Test"
                                     "ex:name" "test"}})
                       :headers json-headers}
          txn-res     (api-post :transact txn-req)
          _           (assert (= 200 (:status txn-res)))

          query-req   {:body
                       (json/stringify
                        {"@context" test-system/default-context
                         "from"     ledger-name
                         "select"   ["?name"]
                         "where"    {"id"      "?person"
                                     "ex:name" "?name"}})
                       :headers json-headers}
          query-res   (api-post :query query-req)]

      (is (= 200 (:status query-res)))
      (is (str/includes? (get-in query-res [:headers "content-type"]) "application/json"))

      (let [results (json/parse (:body query-res) false)]
        (is (vector? results))
        (is (= 1 (count results))))))

  (testing "Streaming query with metadata tracking"
    (let [ledger-name (create-rand-ledger "streaming-meta-test")
          txn-req     {:body
                       (json/stringify
                        {"ledger"   ledger-name
                         "@context" test-system/default-context
                         "insert"   {"id"      "ex:test"
                                     "type"    "schema:Test"
                                     "ex:name" "test"}})
                       :headers json-headers}
          txn-res     (api-post :transact txn-req)
          _           (assert (= 200 (:status txn-res)))

          query-req   {:body
                       (json/stringify
                        {"@context" test-system/default-context
                         "from"     ledger-name
                         "select"   ["?name"]
                         "where"    {"id"      "?person"
                                     "ex:name" "?name"}})
                       :headers (assoc json-headers
                                       "accept" "application/x-ndjson"
                                       "fluree-track-fuel" "true"
                                       "fluree-track-time" "true")}
          query-res   (api-post :query query-req)]

      (is (= 206 (:status query-res)))

      (let [lines (-> query-res :body str/split-lines)
            results (map #(json/parse % false) lines)
            data-results (butlast results)
            meta-result (last results)]
        ;; Last line should be metadata with _fluree-meta key
        (is (contains? meta-result "_fluree-meta"))
        (is (contains? (get meta-result "_fluree-meta") "fuel"))
        (is (contains? (get meta-result "_fluree-meta") "time"))
        ;; All other lines should be vectors (tuples)
        (is (every? vector? data-results)))))

  (testing "Streaming query with LIMIT"
    (let [ledger-name (create-rand-ledger "streaming-limit-test")
          txn-req     {:body
                       (json/stringify
                        {"ledger"   ledger-name
                         "@context" test-system/default-context
                         "insert"   (for [i (range 10)]
                                      {"id"      (str "ex:person" i)
                                       "type"    "schema:Person"
                                       "ex:name" (str "Person " i)})})
                       :headers json-headers}
          txn-res     (api-post :transact txn-req)
          _           (assert (= 200 (:status txn-res)))

          query-req   {:body
                       (json/stringify
                        {"@context" test-system/default-context
                         "from"     ledger-name
                         "select"   ["?name"]
                         "where"    {"id"      "?person"
                                     "type"    "schema:Person"
                                     "ex:name" "?name"}
                         "limit"    3})
                       :headers (assoc json-headers "accept" "application/x-ndjson")}
          query-res   (api-post :query query-req)]

      (is (= 206 (:status query-res)))

      (let [lines (-> query-res :body str/split-lines)
            results (map #(json/parse % false) lines)
            data-results (butlast results)
            meta-result (last results)]
        ;; Should get 3 data results + 1 metadata result (LIMIT 3)
        (is (= 4 (count results)))
        (is (= 3 (count data-results)))
        (is (contains? meta-result "_fluree-meta"))))))
