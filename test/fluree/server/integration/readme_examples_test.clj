(ns fluree.server.integration.readme-examples-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [fluree.db.util.json :as json]
            [fluree.server.integration.test-system :as test-system]))

(use-fixtures :once test-system/run-test-server)

(deftest readme-examples-test
  (testing "README API Examples"

    ;; Example 1: Create a New Ledger (empty)
    (testing "Create ledger without initial data"
      (let [create-response (test-system/api-post
                             :create
                             {:body (json/stringify
                                     {"ledger" "example/ledger"})
                              :headers test-system/json-headers})]
        (is (= 201 (:status create-response)))))

    ;; Example 2: Insert Alice
    (testing "Insert Alice as first data"
      (let [insert-response (test-system/api-post
                             :insert
                             {:body (json/stringify
                                     {"@context" {"schema" "http://schema.org/"
                                                  "ex" "http://example.org/"}
                                      "@graph" [{"@id" "ex:alice"
                                                 "@type" "schema:Person"
                                                 "schema:name" "Alice Johnson"
                                                 "schema:email" "alice@example.com"}]})
                              :headers (assoc test-system/json-headers
                                              "fluree-ledger" "example/ledger")})]
        (is (= 200 (:status insert-response)))))

    ;; Example 3: Insert Additional Data
    (testing "Add Bob who knows Alice"
      (let [insert-response (test-system/api-post
                             :insert
                             {:body (json/stringify
                                     {"@context" {"schema" "http://schema.org/"
                                                  "ex" "http://example.org/"}
                                      "@graph" [{"@id" "ex:bob"
                                                 "@type" "schema:Person"
                                                 "schema:name" "Bob Smith"
                                                 "schema:email" "bob@example.com"
                                                 "schema:knows" {"@id" "ex:alice"}}]})
                              :headers (assoc test-system/json-headers
                                              "fluree-ledger" "example/ledger")})]
        (is (= 200 (:status insert-response))))))

    ;; Example 4: Query Data
  (testing "Query all Person entities"
    (let [query-response (test-system/api-post
                          :query
                          {:body (json/stringify
                                  {"from" "example/ledger"
                                   "@context" {"schema" "http://schema.org/"
                                               "ex" "http://example.org/"}
                                   "select" {"?person" ["*"]}
                                   "where" {"@id" "?person"
                                            "@type" "schema:Person"}})
                           :headers test-system/json-headers})]
      (is (= 200 (:status query-response)))
      (let [results (json/parse (:body query-response) false)
            alice (first (filter #(= "ex:alice" (get % "@id")) results))
            bob (first (filter #(= "ex:bob" (get % "@id")) results))]
        (is (= 2 (count results)))
            ;; Check Alice
        (is (= {"@id" "ex:alice"
                "@type" "schema:Person"
                "schema:name" "Alice Johnson"
                "schema:email" "alice@example.com"}
               alice))
            ;; Check Bob
        (is (= {"@id" "ex:bob"
                "@type" "schema:Person"
                "schema:name" "Bob Smith"
                "schema:email" "bob@example.com"
                "schema:knows" {"@id" "ex:alice"}}
               bob)))))

    ;; Example 5: SPARQL Query
  (testing "SPARQL query for all persons"
    (let [sparql-query "PREFIX schema: <http://schema.org/>
                         PREFIX ex: <http://example.org/>
                         
                         SELECT ?person ?name ?email
                         FROM <example/ledger>
                         WHERE {
                           ?person a schema:Person ;
                                   schema:name ?name ;
                                   schema:email ?email .
                         }"
          sparql-response (test-system/api-post
                           :query
                           {:body sparql-query
                            :headers test-system/sparql-results-headers})]
      (is (= 200 (:status sparql-response)))
      (let [body (json/parse (:body sparql-response) false)]
          ;; SPARQL returns results in a specific format
        (is (map? body))
        (is (contains? body "head"))
        (is (contains? body "results"))
        (let [bindings (get-in body ["results" "bindings"])]
          (is (= 2 (count bindings)))
            ;; Check that we have results for both Alice and Bob
          (let [names (set (map #(get-in % ["name" "value"]) bindings))]
            (is (= #{"Alice Johnson" "Bob Smith"} names)))))))

    ;; Example 6: Insert Data (Charlie)
  (testing "Insert Charlie using /insert endpoint"
    (let [insert-response (test-system/api-post
                           :insert
                           {:body (json/stringify
                                   {"@context" {"schema" "http://schema.org/"
                                                "ex" "http://example.org/"}
                                    "@graph" [{"@id" "ex:charlie"
                                               "@type" "schema:Person"
                                               "schema:name" "Charlie Brown"
                                               "schema:email" "charlie@example.com"}]})
                            :headers (assoc test-system/json-headers
                                            "fluree-ledger" "example/ledger")})]
      (is (= 200 (:status insert-response)))))

    ;; Example 7: Update Data
  (testing "Update Alice's email"
    (let [update-response (test-system/api-post
                           :update
                           {:body (json/stringify
                                   {"@context" {"schema" "http://schema.org/"
                                                "ex" "http://example.org/"}
                                    "where" {"@id" "?person"
                                             "schema:email" "alice@example.com"}
                                    "delete" {"@id" "?person"
                                              "schema:email" "alice@example.com"}
                                    "insert" {"@id" "?person"
                                              "schema:email" "alice@newdomain.com"}})
                            :headers (assoc test-system/json-headers
                                            "fluree-ledger" "example/ledger")})]
      (is (= 200 (:status update-response)))))

    ;; Example 8: Upsert Data
  (testing "Upsert Alice's age and add Diana"
    (let [upsert-response (test-system/api-post
                           :upsert
                           {:body (json/stringify
                                   {"@context" {"schema" "http://schema.org/"
                                                "ex" "http://example.org/"}
                                    "@graph" [{"@id" "ex:alice"
                                               "schema:age" 43}
                                              {"@id" "ex:diana"
                                               "@type" "schema:Person"
                                               "schema:name" "Diana Prince"
                                               "schema:email" "diana@example.com"}]})
                            :headers (assoc test-system/json-headers
                                            "fluree-ledger" "example/ledger")})
          verify-response (test-system/api-post
                           :query
                           {:body (json/stringify
                                   {"from" "example/ledger"
                                    "@context" {"schema" "http://schema.org/"
                                                "ex" "http://example.org/"}
                                    "select" {"?person" ["*"]}
                                    "where" {"@id" "?person"
                                             "@type" "schema:Person"}})
                            :headers test-system/json-headers})]
      (is (= 200 (:status upsert-response)))
          ;; Verify the upsert worked
      (is (= 200 (:status verify-response)))
      (let [results (json/parse (:body verify-response) false)
            alice (first (filter #(= "ex:alice" (get % "@id")) results))
            diana (first (filter #(= "ex:diana" (get % "@id")) results))]
        (is (= 4 (count results))) ;; Alice, Bob, Charlie, Diana
            ;; Check Alice has age now and email is still updated
        (is (= {"@id" "ex:alice"
                "@type" "schema:Person"
                "schema:name" "Alice Johnson"
                "schema:email" "alice@newdomain.com"
                "schema:age" 43}
               alice))
            ;; Check Diana was added
        (is (= {"@id" "ex:diana"
                "@type" "schema:Person"
                "schema:name" "Diana Prince"
                "schema:email" "diana@example.com"}
               diana)))))

    ;; Example 9: Insert with Turtle Format
  (testing "Insert Emily using Turtle format"
    (let [turtle-data "@prefix schema: <http://schema.org/> .
@prefix ex: <http://example.org/> .

ex:emily a schema:Person ;
    schema:name \"Emily Davis\" ;
    schema:email \"emily@example.com\" ;
    schema:age 28 ."
          insert-response (test-system/api-post
                           :insert
                           {:body turtle-data
                            :headers (assoc test-system/json-headers
                                            "content-type" "text/turtle"
                                            "fluree-ledger" "example/ledger")})]
      (is (= 200 (:status insert-response)))))

    ;; Example 10: Upsert with Turtle Format
  (testing "Upsert Emily's job title and add Frank using Turtle format"
    (let [turtle-data "@prefix schema: <http://schema.org/> .
@prefix ex: <http://example.org/> .

ex:emily schema:jobTitle \"Senior Engineer\" .
ex:frank a schema:Person ;
    schema:name \"Frank Wilson\" ;
    schema:email \"frank@example.com\" ."
          upsert-response (test-system/api-post
                           :upsert
                           {:body turtle-data
                            :headers (assoc test-system/json-headers
                                            "content-type" "text/turtle"
                                            "fluree-ledger" "example/ledger")})
          verify-response (test-system/api-post
                           :query
                           {:body (json/stringify
                                   {"from" "example/ledger"
                                    "@context" {"schema" "http://schema.org/"
                                                "ex" "http://example.org/"}
                                    "select" {"?person" ["*"]}
                                    "where" {"@id" "?person"
                                             "@type" "schema:Person"}})
                            :headers test-system/json-headers})]
      (is (= 200 (:status upsert-response)))
          ;; Verify the upsert worked
      (is (= 200 (:status verify-response)))
      (let [results (json/parse (:body verify-response) false)
            emily (first (filter #(= "ex:emily" (get % "@id")) results))
            frank (first (filter #(= "ex:frank" (get % "@id")) results))]
        (is (= 6 (count results))) ;; Alice, Bob, Charlie, Diana, Emily, Frank
            ;; Check Emily has job title now and original data preserved
        (is (= {"@id" "ex:emily"
                "@type" "schema:Person"
                "schema:name" "Emily Davis"
                "schema:email" "emily@example.com"
                "schema:age" 28
                "schema:jobTitle" "Senior Engineer"}
               emily))
            ;; Check Frank was added
        (is (= {"@id" "ex:frank"
                "@type" "schema:Person"
                "schema:name" "Frank Wilson"
                "schema:email" "frank@example.com"}
               frank))))

    ;; Example 11: Time Travel Query at t=2
    (testing "Query at transaction 2 (before email update)"
      (let [time-query-response (test-system/api-post
                                 :query
                                 {:body (json/stringify
                                         {"from" "example/ledger"
                                          "t" 2
                                          "@context" {"schema" "http://schema.org/"
                                                      "ex" "http://example.org/"}
                                          "select" {"?person" ["*"]}
                                          "where" {"@id" "?person"
                                                   "@type" "schema:Person"}})
                                  :headers test-system/json-headers})]
        (is (= 200 (:status time-query-response)))
        (let [results (json/parse (:body time-query-response) false)
              alice (first (filter #(= "ex:alice" (get % "@id")) results))]
          (is (= 2 (count results)))
            ;; At t=2, Alice should still have her old email
          (is (= {"@id" "ex:alice"
                  "@type" "schema:Person"
                  "schema:name" "Alice Johnson"
                  "schema:email" "alice@example.com"}
                 alice)))))

    ;; Example 12: Current state query (after update)
    (testing "Query current state shows updated email"
      (let [current-query-response (test-system/api-post
                                    :query
                                    {:body (json/stringify
                                            {"from" "example/ledger"
                                             "@context" {"schema" "http://schema.org/"
                                                         "ex" "http://example.org/"}
                                             "select" {"?person" ["*"]}
                                             "where" {"@id" "?person"
                                                      "@type" "schema:Person"}})
                                     :headers test-system/json-headers})]
        (is (= 200 (:status current-query-response)))
        (let [results (json/parse (:body current-query-response) false)
              alice (first (filter #(= "ex:alice" (get % "@id")) results))]
          (is (= 6 (count results))) ;; Alice, Bob, Charlie, Diana, Emily, Frank
            ;; Current state should show Alice's new email and age
          (is (= {"@id" "ex:alice"
                  "@type" "schema:Person"
                  "schema:name" "Alice Johnson"
                  "schema:email" "alice@newdomain.com"
                  "schema:age" 43}
                 alice))))) ;; Should have age from upsert

    ;; Example 13: History Query for a Specific Subject
    (testing "Query history for Alice"
      (let [history-response (test-system/api-post
                              :history
                              {:body (json/stringify
                                      {"from" "example/ledger"
                                       "commit-details" true
                                       "history" ["ex:alice"]
                                       "t" {"from" 1}
                                       "@context" {"schema" "http://schema.org/"
                                                   "ex" "http://example.org/"
                                                   "f" "https://ns.flur.ee/ledger#"}})
                               :headers test-system/json-headers})]
        (is (= 200 (:status history-response)))
        (let [history (json/parse (:body history-response) false)]
          (is (= 3 (count history))) ;; create, update email, upsert age
          ;; Verify each commit has the expected structure and transaction numbers
          (is (= [1 4 5] (map #(get % "f:t") history)))
          ;; Verify all commits have proper structure
          (is (every? #(and (contains? % "f:commit")
                            (contains? % "f:assert")
                            (contains? % "f:retract")
                            (= "example/ledger" (get-in % ["f:commit" "f:alias"]))
                            (= "main" (get-in % ["f:commit" "f:branch"])))
                      history)))))))