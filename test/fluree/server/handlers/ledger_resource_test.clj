(ns fluree.server.handlers.ledger-resource-test
  (:require [clojure.test :refer [deftest is testing]]
            [fluree.server.handlers.ledger-resource :as lr]))

(deftest parse-ledger-operation-test
  (testing "Parse ledger operation from path"
    (testing "Simple ledger with operation"
      (is (= {:ledger-name "my-ledger" :operation :insert}
             (lr/parse-ledger-operation "my-ledger/:insert"))))

    (testing "Nested ledger path with operation"
      (is (= {:ledger-name "my/nested/ledger" :operation :update}
             (lr/parse-ledger-operation "my/nested/ledger/:update"))))

    (testing "Ledger path without operation"
      (is (= {:ledger-name "my-ledger" :operation nil}
             (lr/parse-ledger-operation "my-ledger"))))

    (testing "Complex ledger name with slashes"
      (is (= {:ledger-name "org/project/dataset" :operation :history}
             (lr/parse-ledger-operation "org/project/dataset/:history"))))

    (testing "All supported operations"
      (doseq [op [:create :insert :upsert :update :delete :history :drop]]
        (let [path (str "test-ledger/:" (name op))]
          (is (= {:ledger-name "test-ledger" :operation op}
                 (lr/parse-ledger-operation path))))))

    (testing "Ledger name with multiple slashes and operation"
      (is (= {:ledger-name "a/b/c/d/e" :operation :history}
             (lr/parse-ledger-operation "a/b/c/d/e/:history"))))))