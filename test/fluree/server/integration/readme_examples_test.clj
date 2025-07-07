(ns fluree.server.integration.readme-examples-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [fluree.db.util.json :as json]
            [fluree.server.integration.test-system :as test-system]))

(use-fixtures :once test-system/run-test-server)

(deftest readme-examples-test
  (testing "README API Examples"

    ;; Example 1: Create a New Ledger with an Initial Commit
    (testing "Create ledger with initial data"
      (let [create-response (test-system/api-post
                             :create
                             {:body (json/stringify
                                     {"ledger" "example/ledger"
                                      "@context" {"schema" "http://schema.org/"
                                                  "ex" "http://example.org/"}
                                      "insert" [{"@id" "ex:alice"
                                                 "@type" "schema:Person"
                                                 "schema:name" "Alice Johnson"
                                                 "schema:email" "alice@example.com"}]})
                              :headers test-system/json-headers})]
        (is (= 201 (:status create-response)))
        (let [body (json/parse (:body create-response) false)]
          (is (= "example/ledger" (get body "ledger")))
          (is (= 1 (get body "t")))
          (is (string? (get body "tx-id")))
          (is (map? (get body "commit")))
          (is (string? (get-in body ["commit" "address"])))
          (is (string? (get-in body ["commit" "hash"]))))))

    ;; Example 2: Insert Additional Data
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
        (is (= 200 (:status insert-response)))
        (let [body (json/parse (:body insert-response) false)]
          (is (= "example/ledger" (get body "ledger")))
          (is (= 2 (get body "t")))
          (is (string? (get body "tx-id")))
          (is (map? (get body "commit")))
          (is (string? (get-in body ["commit" "address"])))
          (is (string? (get-in body ["commit" "hash"]))))))

    ;; Example 3: Query Data
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
        (let [results (json/parse (:body query-response) false)]
          (is (= 2 (count results)))
          ;; Check Alice
          (let [alice (first (filter #(= "ex:alice" (get % "@id")) results))]
            (is alice)
            (when alice
              (is (= "ex:alice" (get alice "@id")))
              (is (= "schema:Person" (get alice "@type")))
              (is (= "Alice Johnson" (get alice "schema:name")))
              (is (= "alice@example.com" (get alice "schema:email")))))
          ;; Check Bob
          (let [bob (first (filter #(= "ex:bob" (get % "@id")) results))]
            (is bob)
            (when bob
              (is (= "ex:bob" (get bob "@id")))
              (is (= "schema:Person" (get bob "@type")))
              (is (= "Bob Smith" (get bob "schema:name")))
              (is (= "bob@example.com" (get bob "schema:email")))
              (is (= {"@id" "ex:alice"} (get bob "schema:knows"))))))))

    ;; Example 4: SPARQL Query
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
              (is (contains? names "Alice Johnson"))
              (is (contains? names "Bob Smith")))))))

    ;; Example 5: Insert Data (Charlie)
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
        (is (= 200 (:status insert-response)))
        (let [body (json/parse (:body insert-response) false)]
          (is (= "example/ledger" (get body "ledger")))
          (is (= 3 (get body "t")))
          (is (string? (get body "tx-id")))
          (is (map? (get body "commit")))
          (is (string? (get-in body ["commit" "address"])))
          (is (string? (get-in body ["commit" "hash"]))))))

    ;; Example 6: Update Data
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
        (is (= 200 (:status update-response)))
        (let [body (json/parse (:body update-response) false)]
          (is (= "example/ledger" (get body "ledger")))
          (is (= 4 (get body "t")))
          (is (string? (get body "tx-id")))
          (is (map? (get body "commit")))
          (is (string? (get-in body ["commit" "address"])))
          (is (string? (get-in body ["commit" "hash"]))))))

    ;; Example 7: Upsert Data
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
        (let [body (json/parse (:body upsert-response) false)]
          (is (= "example/ledger" (get body "ledger")))
          (is (= 5 (get body "t")))
          (is (string? (get body "tx-id")))
          (is (map? (get body "commit")))
          (is (string? (get-in body ["commit" "address"])))
          (is (string? (get-in body ["commit" "hash"]))))
        ;; Verify the upsert worked
        (is (= 200 (:status verify-response)))
        (let [results (json/parse (:body verify-response) false)]
          (is (= 4 (count results))) ;; Alice, Bob, Charlie, Diana
          ;; Check Alice has age now
          (let [alice (first (filter #(= "ex:alice" (get % "@id")) results))]
            (when alice
              (is (= 43 (get alice "schema:age")))
              (is (= "alice@newdomain.com" (get alice "schema:email"))))) ;; Email should still be updated
          ;; Check Diana was added
          (let [diana (first (filter #(= "ex:diana" (get % "@id")) results))]
            (when diana
              (is (= "Diana Prince" (get diana "schema:name")))
              (is (= "diana@example.com" (get diana "schema:email"))))))))

    ;; Example 8: Time Travel Query at t=2
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
        (let [results (json/parse (:body time-query-response) false)]
          (is (= 2 (count results)))
          ;; At t=2, Alice should still have her old email
          (let [alice (first (filter #(= "ex:alice" (get % "@id")) results))]
            (when alice
              (is (= "alice@example.com" (get alice "schema:email"))))))))

    ;; Example 7: Current state query (after update)
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
        (let [results (json/parse (:body current-query-response) false)]
          (is (= 4 (count results))) ;; Alice, Bob, Charlie, Diana
          ;; Current state should show Alice's new email and age
          (let [alice (first (filter #(= "ex:alice" (get % "@id")) results))]
            (when alice
              (is (= "alice@newdomain.com" (get alice "schema:email")))
              (is (= 43 (get alice "schema:age"))))))))) ;; Should have age from upsert

    ;; Example 10: History Query for a Specific Subject
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
          (is (sequential? history))
          ;; We should have 3 commits: create, update email, and upsert age
          (is (= 3 (count history)))

          ;; First commit (t=1) - Alice created
          (let [first-commit (first history)]
            (is (contains? first-commit "f:commit"))
            (is (= 1 (get first-commit "f:t")))
            (let [commit-data (get first-commit "f:commit")]
              (is (= "example/ledger" (get commit-data "f:alias")))
              (is (= "main" (get commit-data "f:branch")))
              (is (= 1 (get commit-data "f:v")))
              (let [assert-data (get first-commit "f:assert")]
                (is (vector? assert-data))
                (is (= 1 (count assert-data)))
                (let [alice-assert (first assert-data)]
                  (is (= "ex:alice" (get alice-assert "@id")))
                  (is (= "schema:Person" (get alice-assert "@type")))
                  (is (= "Alice Johnson" (get alice-assert "schema:name")))
                  (is (= "alice@example.com" (get alice-assert "schema:email")))))
              (is (empty? (get first-commit "f:retract")))))

          ;; Second commit (t=4) - Alice's email updated
          (let [second-commit (second history)]
            (is (contains? second-commit "f:commit"))
            (is (= 4 (get second-commit "f:t")))
            (let [commit-data (get second-commit "f:commit")]
              (is (= "example/ledger" (get commit-data "f:alias")))
              (is (= "main" (get commit-data "f:branch")))
              (is (map? (get commit-data "f:previous"))) ; has previous commit
              (is (= 1 (get commit-data "f:v")))
              ;; Check assertions
              (let [assert-data (get second-commit "f:assert")]
                (is (vector? assert-data))
                (is (= 1 (count assert-data)))
                (let [alice-assert (first assert-data)]
                  (is (= "ex:alice" (get alice-assert "@id")))
                  (is (= "alice@newdomain.com" (get alice-assert "schema:email")))))
              ;; Check retractions
              (let [retract-data (get second-commit "f:retract")]
                (is (vector? retract-data))
                (is (= 1 (count retract-data)))
                (let [alice-retract (first retract-data)]
                  (is (= "ex:alice" (get alice-retract "@id")))
                  (is (= "alice@example.com" (get alice-retract "schema:email")))))))
          
          ;; Third commit (t=5) - Alice's age added via upsert
          (let [third-commit (nth history 2)]
            (is (contains? third-commit "f:commit"))
            (is (= 5 (get third-commit "f:t")))
            (let [commit-data (get third-commit "f:commit")]
              (is (= "example/ledger" (get commit-data "f:alias")))
              (is (= "main" (get commit-data "f:branch")))
              (is (map? (get commit-data "f:previous"))) ; has previous commit
              (is (= 1 (get commit-data "f:v")))
              ;; Check assertions - should have Alice's age
              (let [assert-data (get third-commit "f:assert")]
                (is (vector? assert-data))
                (is (= 1 (count assert-data)))
                (let [alice-assert (first assert-data)]
                  (is (= "ex:alice" (get alice-assert "@id")))
                  (is (= 43 (get alice-assert "schema:age")))))))))))