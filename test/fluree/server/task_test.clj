(ns fluree.server.task-test
  (:require [clojure.core.async :as async]
            [clojure.test :refer [deftest is testing]]
            [fluree.db.api :as fluree]
            [fluree.db.util.json :as json]
            [fluree.server.integration.test-system :refer [api-post json-headers] :as test-system]
            [fluree.server.system :as system]
            [fluree.server.task :as task]
            [test-with-files.tools :refer [with-tmp-dir] :as twf]))

(defn test-config
  [file-path]
  (-> (test-system/file-server-config file-path)
      (update "@graph" conj {"@id"             "testConnection"
                             ;; configure low novelty to generate indexes
                             "noveltyMinBytes" 1000})))

(defn create-ledger-with-data [conn ledger-name item-count]
  ; Create ledger
  @(fluree/create conn ledger-name)
  ; Add data to trigger indexing
  (let [txn {"@context" {"ex" "http://example.org/"}
             "ledger" ledger-name
             "insert" (vec (for [i (range item-count)]
                             {"@id" (str "ex:item-" i)
                              "@type" "ex:Item"
                              "ex:value" i}))}]
    @(fluree/transact! conn txn)))

(deftest reindex-task
  (test-system/set-server-ports)
  (with-tmp-dir storage-path {::twf/delete-dir false}
    (testing "reindex with specific ledger alias"
      (let [system (system/start-config (test-config storage-path))
            conn-key (first (filter #(isa? % :fluree.db/connection) (keys system)))
            conn (get system conn-key)]
        (create-ledger-with-data conn "test-ledger" 100)
        (let [result (async/<!! (task/reindex conn "test-ledger"))]
          (is (= :reindexed result)
              "Should return :reindexed on successful reindex"))
        (system/stop system)))

    (testing "reindex with --all flag"
      (let [system (system/start-config (test-config storage-path))
            conn-key (first (filter #(isa? % :fluree.db/connection) (keys system)))
            conn (get system conn-key)]
        (create-ledger-with-data conn "test-ledger-1" 50)
        (create-ledger-with-data conn "test-ledger-2" 60)
        (is (= :reindexed-all (async/<!! (task/reindex conn "--all"))))
        (system/stop system)))

    (testing "can reload reindexed ledgers"
      (let [system (system/start-config (test-config storage-path))
            query {"@context" {"ex" "http://example.org/"}
                   "where" [{"@id" "?s", "ex:value" "?v"}],
                   "select" "(as (sum ?v) ?sum)"}]
        (is (= [(reduce + (range 100))]
               (-> (api-post :query {:body (-> query (assoc "from" "test-ledger") (json/stringify))
                                     :headers json-headers})
                   :body
                   (json/parse false))))
        (is (= [(reduce + (range 50))]
               (-> (api-post :query {:body (-> query (assoc "from" "test-ledger-1") (json/stringify))
                                     :headers json-headers})
                   :body
                   (json/parse false))))
        (is (= [(reduce + (range 60))]
               (-> (api-post :query {:body (-> query (assoc "from" "test-ledger-2") (json/stringify))
                                     :headers json-headers})
                   :body
                   (json/parse false))))
        (system/stop system)))))
