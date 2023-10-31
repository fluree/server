(ns fluree.server.integration.default-context-test
  (:require [clojure.test :refer :all]
            [fluree.server.integration.test-system :refer :all]
            [jsonista.core :as json]))

(use-fixtures :once run-test-server)

(deftest ^:integration get-default-context-test
  (testing "can retrieve default context for a ledger"
    (let [ledger-name         (create-rand-ledger "get-default-context-test")
          default-context-req {:body    (json/write-value-as-string
                                          {:ledger ledger-name})
                               :headers json-headers}
          default-context-res (api-get :defaultContext default-context-req)]
      (is (= 200 (:status default-context-res)))
      (is (= {"ex"     "http://example.com/"
              "f"      "https://ns.flur.ee/ledger#"
              "foo"    "http://foobar.com/"
              "id"     "@id"
              "rdf"    "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
              "schema" "http://schema.org/"
              "type"   "@type"
              "graph"  "@graph"}
             (-> default-context-res :body json/read-value)))))

  (testing "can retrieve default context at a specific t"
    (let [ledger-name          (create-rand-ledger "get-default-context-test")
          default-context-req  {:body    (json/write-value-as-string
                                           {:ledger ledger-name})
                                :headers json-headers}
          default-context-res  (api-get :defaultContext default-context-req)
          default-context0     (-> default-context-res :body json/read-value)
          default-context1     (-> default-context0
                                   (assoc "ex-new"
                                          "http://example.com/")
                                   (dissoc "ex"))
          default-context2     (-> default-context1
                                   (assoc "foo-new"
                                          "http://foobar.com/")
                                   (dissoc "foo"))
          txn0-req             {:body
                                (json/write-value-as-string
                                  {"@context"         "https://ns.flur.ee"
                                   "ledger"         ledger-name
                                   "insert"           [{"id"      "ex:nobody"
                                                        "ex:name" "Nobody"}]
                                   "defaultContext" default-context1})
                                :headers json-headers}
          txn0-res             (api-post :transact txn0-req)
          _                    (assert (= 200 (:status txn0-res)))
          txn1-req             {:body
                                (json/write-value-as-string
                                  {"@context" "https://ns.flur.ee"
                                  "ledger"         ledger-name
                                  "insert"           [{"id"      "ex:somebody"
                                                       "ex:name" "Somebody"}]
                                  "defaultContext" default-context2})
                                :headers json-headers}
          txn1-res             (api-post :transact txn1-req)
          _                    (assert (= 200 (:status txn1-res)))
          default-context1-req {:body    (json/write-value-as-string
                                          {:ledger ledger-name
                                           :t      1})
                                :headers json-headers}
          default-context1-res (api-get :defaultContext default-context1-req)
          default-context2-req {:body    (json/write-value-as-string
                                           {:ledger ledger-name
                                            :t      2})
                                :headers json-headers}
          default-context2-res (api-get :defaultContext default-context2-req)
          default-context3-req {:body    (json/write-value-as-string
                                           {:ledger ledger-name
                                            :t      3})
                                :headers json-headers}
          default-context3-res (api-get :defaultContext default-context3-req)]
      (is (= {"ex"     "http://example.com/"
              "f"      "https://ns.flur.ee/ledger#"
              "foo"    "http://foobar.com/"
              "id"     "@id"
              "rdf"    "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
              "schema" "http://schema.org/"
              "type"   "@type"
              "graph"  "@graph"}
             (-> default-context1-res :body json/read-value)))
      (is (= {"ex-new" "http://example.com/"
              "f"      "https://ns.flur.ee/ledger#"
              "foo"    "http://foobar.com/"
              "id"     "@id"
              "rdf"    "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
              "schema" "http://schema.org/"
              "type"   "@type"
              "graph"  "@graph"}
             (-> default-context2-res :body json/read-value)))
      (is (= {"ex-new"  "http://example.com/"
              "f"       "https://ns.flur.ee/ledger#"
              "foo-new" "http://foobar.com/"
              "id"      "@id"
              "rdf"     "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
              "schema"  "http://schema.org/"
              "type"    "@type"
              "graph"   "@graph"}
             (-> default-context3-res :body json/read-value))))))

(deftest ^:integration update-default-context-test
  (testing "can update default context for a ledger"
    (let [ledger-name          (create-rand-ledger "get-default-context-test")
          default-context-req  {:body    (json/write-value-as-string
                                           {:ledger ledger-name})
                                :headers json-headers}
          default-context-res  (api-get :defaultContext default-context-req)
          default-context-0    (-> default-context-res :body json/read-value)
          update-req           {:body    (json/write-value-as-string
                                           {"ledger" ledger-name
                                            "@context" "https://ns.flur.ee"
                                            "insert"   [{"ex:name" "Foo"}]
                                            "defaultContext"
                                            (-> default-context-0
                                                (assoc "foo-new"
                                                       (get default-context-0 "foo"))
                                                (dissoc "foo"))})
                                :headers json-headers}
          update-res           (api-post :transact update-req)
          _                    (assert (= 200 (:status update-res)))
          default-context-res' (api-get :defaultContext default-context-req)
          default-context-1    (-> default-context-res' :body json/read-value)]
      (is (= 200 (:status update-res)))
      (is (= {"foo-new" "http://foobar.com/"
              "ex"      "http://example.com/"
              "f"       "https://ns.flur.ee/ledger#"
              "id"      "@id"
              "rdf"     "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
              "schema"  "http://schema.org/"
              "type"    "@type"
              "graph"   "@graph"}
             default-context-1)))))
