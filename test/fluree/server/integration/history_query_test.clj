(ns fluree.server.integration.history-query-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [fluree.server.integration.test-system
             :as test-system
             :refer [api-post json-headers run-test-server]]
            [fluree.db.util.json :as json]))

(use-fixtures :once run-test-server)

(def commit-id-regex
  (re-pattern "fluree:commit:sha256:[a-z2-7]{51,53}"))

(def mem-addr-regex
  (re-pattern "fluree:memory://[a-z2-7]{51,53}"))

(def db-id-regex
  (re-pattern "fluree:db:sha256:[a-z2-7]{51,53}"))

(deftest ^:integration ^:json history-query-json-test
  (testing "basic JSON history query works"
    (let [ledger-name   "history-query-json-test"
          txn-req       {:body
                         (json/stringify
                          {"ledger"   ledger-name
                           "@context" test-system/default-context
                           "insert"   [{"id"      "ex:query-test"
                                        "type"    "schema:Test"
                                        "ex:name" "query-test"}]})
                         :headers json-headers}
          txn-res       (api-post :create txn-req)
          _             (assert (= 201 (:status txn-res)))
          txn2-req      {:body
                         (json/stringify
                          {"ledger"   ledger-name
                           "@context" test-system/default-context
                           "insert"   [{"id"           "ex:query-test"
                                        "ex:test-type" "integration"}]})
                         :headers json-headers}
          txn2-res      (api-post :transact txn2-req)
          _             (assert (= 200 (:status txn2-res)))
          query-req     {:body
                         (json/stringify
                          {"@context"       test-system/default-context
                           "from"           ledger-name
                           "commit-details" true
                           "t"              {"at" "latest"}})
                         :headers json-headers}
          query-res     (api-post :history query-req)
          query-results (-> query-res :body (json/parse false))]
      (is (= 200 (:status query-res))
          (str "History query response was: " (pr-str query-res)))
      (let [history-expectations
            {["id"]                  #(re-matches commit-id-regex %)
             ["f:address"]           #(re-matches mem-addr-regex %)
             ["f:alias"]             #(= % ledger-name)
             ["f:branch"]            #(= % "main")
             ["f:previous"]          #(or (nil? %)
                                          (re-matches commit-id-regex (get % "id")))
             ["f:time"]              pos-int?
             ["f:v"]                 nat-int?
             ["f:data" "f:address"]  #(re-matches mem-addr-regex %)
             ["f:data" "f:assert"]   #(and (vector? %) (every? map? %))
             ["f:data" "f:retract"]  #(and (vector? %) (every? map? %))
             ["f:data" "f:flakes"]   pos-int?
             ["f:data" "f:size"]     pos-int?
             ["f:data" "f:t"]        pos-int?}]
        (is (every? #(every? (fn [[kp valid?]]
                               (let [actual (get-in % kp)]
                                 (or
                                  (valid? actual)
                                  (println "invalid:" (pr-str actual)))))
                             history-expectations)
                    (map #(get % "f:commit") query-results)))))))

#_(deftest ^:integration ^:edn history-query-edn-test
    (testing "basic EDN history query works"
      (let [ledger-name   "history-query-edn-test"
            txn-req       {:body
                           (pr-str
                            {:ledger ledger-name
                             :txn    [{:id      :ex/query-test
                                       :type    :schema/Test
                                       :ex/name "query-test"}]})
                           :headers edn-headers}
            txn-res       (api-post :create txn-req)
            _             (assert (= 201 (:status txn-res)))
            txn2-req      {:body
                           (pr-str
                            {:ledger ledger-name
                             :txn    [{:id           :ex/query-test
                                       :ex/test-type "integration"}]})
                           :headers edn-headers}
            txn2-res      (api-post :transact txn2-req)
            _             (assert (= 200 (:status txn2-res)))
            query-req     {:body
                           (pr-str
                            {:from           ledger-name
                             :commit-details true
                             :t              {:from 1}})
                           :headers edn-headers}
            query-res     (api-post :history query-req)
            query-results (-> query-res :body edn/read-string)]
        (is (= 200 (:status query-res))
            (str "History query response was: " (pr-str query-res)))
        (let [history-expectations
              {[:id]                 #(re-matches commit-id-regex %)
               [:f/address]          #(re-matches mem-addr-regex %)
               [:f/alias]            #(= % ledger-name)
               [:f/branch]           #(= % "main")
               [:f/defaultContext]   #(re-matches context-id-regex (:id %))
               [:f/previous]         #(or (nil? %)
                                          (re-matches commit-id-regex (:id %)))
               [:f/time]             pos-int?
               [:f/v]                nat-int?
               [:f/data :f/address]  #(re-matches mem-addr-regex %)
               [:f/data :f/assert]   #(and (vector? %) (every? map? %))
               [:f/data :f/retract]  #(and (vector? %) (every? map? %))
               [:f/data :f/flakes]   pos-int?
               [:f/data :f/size]     pos-int?
               [:f/data :f/t]        pos-int?
               [:f/data :f/previous] #(or (nil? %)
                                          (re-matches db-id-regex (:id %)))}]
          (is (every? #(every? (fn [[kp valid?]]
                                 (let [actual (get-in % kp)]
                                   (or
                                    (valid? actual)
                                    (println "invalid:" (pr-str actual)))))
                               history-expectations)
                      (map :f/commit query-results)))))))
