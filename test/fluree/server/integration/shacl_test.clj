(ns fluree.server.integration.shacl-test
  (:require [clojure.test :refer [deftest is testing]]
            [fluree.server.integration.test-system :refer [api-post json-headers] :as test-system]
            [fluree.server.system :as system]
            [jsonista.core :as json]
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
               (-> (api-post :create {:body (json/write-value-as-string create-req) :headers json-headers})
                   :status)))
        (is (= 422
               (-> (api-post :transact {:body (json/write-value-as-string txn-req) :headers json-headers})
                   :status))))
      (testing "validation works on server restart"
        (system/stop server)
        (let [new-server (system/start-config (test-system/file-server-config storage-path))]
          (is (= 422
                 (-> (api-post :transact {:body (json/write-value-as-string txn-req) :headers json-headers})
                     :status)))
          (system/stop new-server))))))
