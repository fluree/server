(ns fluree.server.integration.remote-system-test
  (:require [clojure.test :refer [are deftest is testing use-fixtures]]
            [fluree.db.util.log :as log]
            [fluree.server.integration.test-system :as test-system
             :refer [api-post create-rand-ledger json-headers]]
            [fluree.server.system :as system]
            [jsonista.core :as json]))

(def all-ports (test-system/find-open-ports 3))
(def authors-port (nth all-ports 0))
(def books-port (nth all-ports 1))
(def movies-port (nth all-ports 2))

(def authors-storage-identifier "ee.flur/authorsSystemMemory")
(def books-storage-identifier "ee.flur/booksSystemMemory")
(def movies-storage-identifier "ee.flur/moviesSystemMemory")

(def authors-config {"@context" {"@base"    "https://ns.flur.ee/dev/config/authors/",
                                 "@vocab"   "https://ns.flur.ee/system#",
                                 "books"    "https://ns.flur.ee/dev/config/books/"
                                 "movies"   "https://ns.flur.ee/dev/config/movies/"
                                 "profiles" {"@container" ["@graph", "@id"]}}
                     "@id"      "testSystem"
                     "@graph"   [{"@id"               "memoryStorage"
                                  "@type"             "Storage"
                                  "addressIdentifier" authors-storage-identifier}

                                 {"@id"                "books:system"
                                  "@type"              "System"
                                  "serverUrls"         [(str "http://localhost:" books-port)]
                                  "addressIdentifiers" [books-storage-identifier]}

                                 {"@id"                "movies:system"
                                  "@type"              "System"
                                  "serverUrls"         [(str "http://localhost:" movies-port)]
                                  "addressIdentifiers" [movies-storage-identifier]}

                                 {"@id"              "testConnection"
                                  "@type"            "Connection"
                                  "parallelism"      1
                                  "cacheMaxMb"       100
                                  "commitStorage"    {"@id" "memoryStorage"}
                                  "indexStorage"     {"@id" "memoryStorage"}
                                  "primaryPublisher" {"@type"   "Publisher"
                                                      "storage" {"@id" "memoryStorage"}}
                                  "remoteSystems"    [{"@id" "books:system"} {"@id" "movies:system"}]}

                                 {"@id"               "testConsensus"
                                  "@type"             "Consensus"
                                  "consensusProtocol" "standalone"
                                  "connection"        {"@id" "testConnection"}
                                  "maxPendingTxns"    16}

                                 {"@id"          "testApiServer"
                                  "@type"        "API"
                                  "httpPort"     authors-port
                                  "maxTxnWaitMs" 45000}]})

(def books-config {"@context" {"@base"    "https://ns.flur.ee/dev/config/books/",
                               "@vocab"   "https://ns.flur.ee/system#",
                               "authors"  "https://ns.flur.ee/dev/config/authors/"
                               "movies"   "https://ns.flur.ee/dev/config/movies/"
                               "profiles" {"@container" ["@graph", "@id"]}}
                   "@id"      "testSystem"
                   "@graph"   [{"@id"               "memoryStorage"
                                "@type"             "Storage"
                                "addressIdentifier" books-storage-identifier}

                               {"@id"                "authors:system"
                                "@type"              "System"
                                "serverUrls"         [(str "http://localhost:" authors-port)]
                                "addressIdentifiers" [authors-storage-identifier]}

                               {"@id"                "movies:system"
                                "@type"              "System"
                                "serverUrls"         [(str "http://localhost:" movies-port)]
                                "addressIdentifiers" [movies-storage-identifier]}

                               {"@id"              "testConnection"
                                "@type"            "Connection"
                                "parallelism"      1
                                "cacheMaxMb"       100
                                "commitStorage"    {"@id" "memoryStorage"}
                                "indexStorage"     {"@id" "memoryStorage"}
                                "primaryPublisher" {"@type"   "Publisher"
                                                    "storage" {"@id" "memoryStorage"}}
                                "remoteSystems"    [{"@id" "authors:system"} {"@id" "movies:system"}]}

                               {"@id"               "testConsensus"
                                "@type"             "Consensus"
                                "consensusProtocol" "standalone"
                                "connection"        {"@id" "testConnection"}
                                "maxPendingTxns"    16}

                               {"@id"          "testApiServer"
                                "@type"        "API"
                                "httpPort"     books-port
                                "maxTxnWaitMs" 45000}]})

(def movies-config {"@context" {"@base"    "https://ns.flur.ee/dev/config/moves/",
                                "@vocab"   "https://ns.flur.ee/system#",
                                "authors"  "https://ns.flur.ee/dev/config/authors/"
                                "books"    "https://ns.flur.ee/dev/config/books/"
                                "profiles" {"@container" ["@graph", "@id"]}}
                    "@id"      "testSystem"
                    "@graph"   [{"@id"               "memoryStorage"
                                 "@type"             "Storage"
                                 "addressIdentifier" movies-storage-identifier}

                                {"@id"                "authors:system"
                                 "@type"              "System"
                                 "serverUrls"         [(str "http://localhost:" authors-port)]
                                 "addressIdentifiers" [authors-storage-identifier]}

                                {"@id"                "books:system"
                                 "@type"              "System"
                                 "serverUrls"         [(str "http://localhost:" books-port)]
                                 "addressIdentifiers" [books-storage-identifier]}

                                {"@id"              "testConnection"
                                 "@type"            "Connection"
                                 "parallelism"      1
                                 "cacheMaxMb"       100
                                 "commitStorage"    {"@id" "memoryStorage"}
                                 "indexStorage"     {"@id" "memoryStorage"}
                                 "primaryPublisher" {"@type"   "Publisher"
                                                     "storage" {"@id" "memoryStorage"}}
                                 "remoteSystems"    [{"@id" "authors:system"} {"@id" "books:system"}]}

                                {"@id"               "testConsensus"
                                 "@type"             "Consensus"
                                 "consensusProtocol" "standalone"
                                 "connection"        {"@id" "testConnection"}
                                 "maxPendingTxns"    16}

                                {"@id"          "testApiServer"
                                 "@type"        "API"
                                 "httpPort"     movies-port
                                 "maxTxnWaitMs" 45000}]})

(defn start-systems
  []
  (let [authors-system (system/start-config authors-config)
        books-system   (system/start-config books-config)
        movies-system  (system/start-config movies-config)]
    [authors-system books-system movies-system]))

(defn run-systems
  [run-tests]
  (let [systems (start-systems)]
    (run-tests)
    (->> systems (map system/stop) dorun)))

(use-fixtures :once run-systems)

(deftest remote-system-test
  (testing "Groups of remote systems"
    (testing "with three separate systems populated with distinct data"
      (let [context {"id"     "@id",
                     "type"   "@type",
                     "ex"     "http://example.org/",
                     "f"      "https://ns.flur.ee/ledger#",
                     "rdf"    "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
                     "rdfs"   "http://www.w3.org/2000/01/rdf-schema#",
                     "schema" "http://schema.org/",
                     "xsd"    "http://www.w3.org/2001/XMLSchema#"}

            authors-ledger      (create-rand-ledger "test/authors" authors-port)
            authors-insert-req  {:body    (json/write-value-as-string
                                           {"@context" ["https://ns.flur.ee" context "https://schema.org"]
                                            "ledger"   authors-ledger
                                            "insert"   [{"@id"   "https://www.wikidata.org/wiki/Q42"
                                                         "@type" "Person"
                                                         "name"  "Douglas Adams"}
                                                        {"@id"   "https://www.wikidata.org/wiki/Q173540"
                                                         "@type" "Person"
                                                         "name"  "Margaret Mitchell"}]})
                                 :headers json-headers}
            authors-insert-resp (api-post :transact authors-insert-req authors-port)

            books-ledger      (create-rand-ledger "test/books" books-port)
            books-insert-req  {:body    (json/write-value-as-string
                                         {"@context" ["https://ns.flur.ee" context "https://schema.org"]
                                          "ledger"   books-ledger
                                          "insert"   [{"id"     "https://www.wikidata.org/wiki/Q3107329",
                                                       "type"   ["Book"],
                                                       "name"   "The Hitchhiker's Guide to the Galaxy",
                                                       "isbn"   "0-330-25864-8",
                                                       "author" {"@id" "https://www.wikidata.org/wiki/Q42"}}
                                                      {"id"     "https://www.wikidata.org/wiki/Q2870",
                                                       "type"   ["Book"],
                                                       "name"   "Gone with the Wind",
                                                       "isbn"   "0-582-41805-4",
                                                       "author" {"@id" "https://www.wikidata.org/wiki/Q173540"}}]})
                               :headers json-headers}
            books-insert-resp (api-post :transact books-insert-req books-port)

            movies-ledger      (create-rand-ledger "test/movies" movies-port)
            movies-insert-req  {:body    (json/write-value-as-string
                                          {"@context" ["https://ns.flur.ee" context "https://schema.org"]
                                           "ledger"   movies-ledger
                                           "insert"   [{"id"                        "https://www.wikidata.org/wiki/Q836821",
                                                        "type"                      ["Movie"],
                                                        "name"                      "The Hitchhiker's Guide to the Galaxy",
                                                        "disambiguatingDescription" "2005 British-American comic science fiction film directed by Garth Jennings",
                                                        "titleEIDR"                 "10.5240/B752-5B47-DBBE-E5D4-5A3F-N",
                                                        "isBasedOn"                 {"id" "https://www.wikidata.org/wiki/Q3107329"}}
                                                       {"id"                        "https://www.wikidata.org/wiki/Q91540",
                                                        "type"                      ["Movie"],
                                                        "name"                      "Back to the Future",
                                                        "disambiguatingDescription" "1985 film by Robert Zemeckis",
                                                        "titleEIDR"                 "10.5240/09A3-1F6E-3538-DF46-5C6F-I",
                                                        "followedBy"                {"id"         "https://www.wikidata.org/wiki/Q109331"
                                                                                     "type"       "Movie"
                                                                                     "name"       "Back to the Future Part II"
                                                                                     "titleEIDR"  "10.5240/5DA5-C386-2911-7E2B-1782-L"
                                                                                     "followedBy" {"id" "https://www.wikidata.org/wiki/Q230552"}}}
                                                       {"id"                        "https://www.wikidata.org/wiki/Q230552"
                                                        "type"                      ["Movie"]
                                                        "name"                      "Back to the Future Part III"
                                                        "disambiguatingDescription" "1990 film by Robert Zemeckis"
                                                        "titleEIDR"                 "10.5240/15F9-F913-FF25-8041-E798-O"}
                                                       {"id"                        "https://www.wikidata.org/wiki/Q2875",
                                                        "type"                      ["Movie"],
                                                        "name"                      "Gone with the Wind",
                                                        "disambiguatingDescription" "1939 film by Victor Fleming",
                                                        "titleEIDR"                 "10.5240/FB0D-0A93-CAD6-8E8D-80C2-4",
                                                        "isBasedOn"                 {"id" "https://www.wikidata.org/wiki/Q2870"}}]})
                                :headers json-headers}
            movies-insert-resp (api-post :transact movies-insert-req movies-port)]

        ;; Adding data to each system is successful
        (are [resp] (= (:status resp) 200)
          authors-insert-resp books-insert-resp movies-insert-resp)

        (testing "can be queried for their combined data set no matter which system the query originates"
          (let [query-req         {:body    (json/write-value-as-string
                                             {"@context" "https://schema.org"
                                              "from"     [authors-ledger books-ledger movies-ledger]
                                              "select"   ["?movieName" "?bookIsbn" "?authorName"]
                                              "where"    {"type"      "Movie"
                                                          "name"      "?movieName"
                                                          "isBasedOn" {"isbn"   "?bookIsbn"
                                                                       "author" {"name" "?authorName"}}}})
                                   :headers json-headers}
                author-query-resp (api-post :query query-req authors-port)
                book-query-resp   (api-post :query query-req books-port)
                movie-query-resp  (api-post :query query-req movies-port)

                correct-answer [["Gone with the Wind" "0-582-41805-4" "Margaret Mitchell"]
                                ["The Hitchhiker's Guide to the Galaxy" "0-330-25864-8" "Douglas Adams"]]]

            ;; No systems returned errors when queried
            (are [resp] (-> resp :status (= 200))
              author-query-resp book-query-resp movie-query-resp)

            ;; Each system returned the same and correct answer when queired
            (are [resp] (-> resp :body json/read-value (= correct-answer))
              author-query-resp book-query-resp movie-query-resp)))))))
