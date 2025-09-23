(ns fluree.server.integration.turtle-format-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [fluree.db.util.json :as json]
            [fluree.server.integration.test-system :as test-system]))

(use-fixtures :once test-system/run-test-server)

(deftest turtle-format-test
  (testing "Turtle format support for insert and upsert endpoints"

    ;; Create a test ledger
    (testing "Create ledger with initial data"
      (let [create-response (test-system/api-post
                             :create
                             {:body (json/stringify
                                     {"ledger" "turtle/test"
                                      "@context" {"schema" "http://schema.org/"
                                                  "ex" "http://example.org/"}
                                      "insert" [{"@id" "ex:alice"
                                                 "@type" "schema:Person"
                                                 "schema:name" "Alice Johnson"
                                                 "schema:email" "alice@example.com"}]})
                              :headers test-system/json-headers})]
        (is (= 201 (:status create-response)))))

    ;; Test turtle insert
    (testing "Insert with Turtle format"
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
                                              "fluree-ledger" "turtle/test")})
            ;; Query to verify the data was inserted
            query-response (test-system/api-post
                            :query
                            {:body (json/stringify
                                    {"from" "turtle/test"
                                     "@context" {"schema" "http://schema.org/"
                                                 "ex" "http://example.org/"}
                                     "select" {"?person" ["*"]}
                                     "where" {"@id" "?person"
                                              "@type" "schema:Person"}})
                             :headers test-system/json-headers})]
        (is (= 200 (:status insert-response)))
        ;; Verify Emily was inserted correctly
        (is (= 200 (:status query-response)))
        (let [results (json/parse (:body query-response) false)
              emily (first (filter #(= "ex:emily" (get % "@id")) results))]
          (is (= 2 (count results))) ;; Alice and Emily
          (is (= {"@id" "ex:emily"
                  "@type" "schema:Person"
                  "schema:name" "Emily Davis"
                  "schema:email" "emily@example.com"
                  "schema:age" 28}
                 emily)))))

    ;; Test turtle upsert
    (testing "Upsert with Turtle format"
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
                                              "fluree-ledger" "turtle/test")})
            verify-response (test-system/api-post
                             :query
                             {:body (json/stringify
                                     {"from" "turtle/test"
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
          (is (= 3 (count results))) ;; Alice, Emily, Frank
          ;; Check Emily has job title now and original data is preserved
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
                 frank)))))))
