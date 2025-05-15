(ns fluree.server.integration.load-state-test
  (:require [clojure.test :refer [deftest is testing]]
            [fluree.db.util.json :as json]
            [fluree.server.integration.test-system :refer [api-post json-headers] :as test-system]
            [fluree.server.system :as system]
            [test-with-files.tools :refer [with-tmp-dir] :as twf]))

(deftest ^:integration shacl-test
  (test-system/set-server-ports)
  (with-tmp-dir storage-path
    (let [server     (system/start-config (test-system/file-server-config storage-path))
          create-req {"@context" {"sh" "http://www.w3.org/ns/shacl#",
                                  "ex" "http://example.org/"},
                      "ledger"   "shacl/uniqueness",
                      "insert"   {"@type"          "sh:NodeShape",
                                  "@id"            "ex:EmailUniquenessShape",
                                  "sh:targetClass" {"@id" "ex:Email"},
                                  "sh:property"    [{"sh:path"     {"sh:inversePath" {"@id" "ex:email"}},
                                                     "sh:maxCount" 1}]}}
          txn-req    {"@context" {"sh" "http://www.w3.org/ns/shacl#",
                                  "ex" "http://example.org/"},
                      "ledger"   "shacl/uniqueness",
                      "insert"   [{"@id"      "andrew",
                                   "ex:email" {"@id" "foobar@flur.ee", "@type" "ex:Email"}}
                                  {"@id"      "derek",
                                   "ex:email" {"@id" "foobar@flur.ee", "@type" "ex:Email"}}]}]
      (testing "validation works on initial server run"
        (is (= 201
               (-> (api-post :create {:body (json/stringify create-req) :headers json-headers})
                   :status)))
        (is (= 422
               (-> (api-post :transact {:body (json/stringify txn-req) :headers json-headers})
                   :status))))
      (testing "validation works on server restart"
        (system/stop server)
        (let [new-server (system/start-config (test-system/file-server-config storage-path))]
          (is (= 422
                 (-> (api-post :transact {:body (json/stringify txn-req) :headers json-headers})
                     :status)))
          (system/stop new-server))))))

(deftest ^:integration policy-test
  (test-system/set-server-ports)
  (with-tmp-dir storage-path
    (let [server (system/start-config (test-system/file-server-config storage-path))
          create {"ledger" "user/ledger",
                  "insert" {"@id"      "freddy",
                            "@type"    "Yeti",
                            "name"     "Freddy",
                            "age"      4,
                            "verified" true},
                  "opts"
                  {"policy" {"@type"                            ["https://ns.flur.ee/ledger#AccessPolicy"],
                             "https://ns.flur.ee/ledger#action" {"@id" "https://ns.flur.ee/ledger#modify"},
                             "https://ns.flur.ee/ledger#query"  {"@type"  "@json",
                                                                 "@value" {}}}}}

          query {"from"   "user/ledger",
                 "where"  [{"@id" "?s", "age" "?age"}],
                 "select" {"?s" ["*"]}}]
      (testing "validation works on initial server run"
        (is (= 201
               (-> (api-post :create {:body (json/stringify create) :headers json-headers})
                   :status)))
        (is (= [{"@id"      "freddy",
                 "age"      4,
                 "name"     "Freddy",
                 "verified" true,
                 "@type"    "Yeti"}]
               (-> (api-post :query {:body (json/stringify query) :headers json-headers})
                   :body
                   (json/parse false)))))
      (testing "validation works on server restart"
        (system/stop server)
        (let [new-server (system/start-config (test-system/file-server-config storage-path))]
          (is (= [{"@id"      "freddy",
                   "age"      4,
                   "name"     "Freddy",
                   "verified" true,
                   "@type"    "Yeti"}]
                 (-> (api-post :query {:body (json/stringify query) :headers json-headers})
                     :body
                     (json/parse false))))
          (system/stop new-server))))))
