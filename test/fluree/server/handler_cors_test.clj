(ns fluree.server.handler-cors-test
  (:require [clojure.test :refer [deftest is testing]]
            [fluree.server.system :as system]))

(deftest test-cors-origin-parsing
  (testing "Parse CORS origins correctly"
    (testing "Wildcard conversion"
      (let [origins (system/parse-cors-origins ["*"])]
        (is (= 1 (count origins)))
        (is (instance? java.util.regex.Pattern (first origins)))
        (is (re-matches (first origins) "http://anything.com"))))

    (testing "Regex pattern"
      (let [origins (system/parse-cors-origins ["^https://.*\\.example\\.com$"])]
        (is (instance? java.util.regex.Pattern (first origins)))
        (is (re-matches (first origins) "https://api.example.com"))
        (is (not (re-matches (first origins) "http://api.example.com")))))

    (testing "Plain string origin"
      (let [origins (system/parse-cors-origins ["http://localhost:3000"])]
        (is (= ["http://localhost:3000"] origins))))))