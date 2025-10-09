(ns fluree.server.handler-cors-test
  (:require [clojure.test :refer [deftest is testing]]
            [fluree.server.handler :as handler]
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

(deftest test-header-constants
  (testing "Header constants are properly defined"
    (testing "standard-request-headers includes common HTTP headers"
      (is (some #{"content-type"} handler/standard-request-headers))
      (is (some #{"accept"} handler/standard-request-headers))
      (is (some #{"authorization"} handler/standard-request-headers)))

    (testing "fluree-request-header-keys includes Fluree-specific headers"
      (is (some #{"fluree-track-meta"} handler/fluree-request-header-keys))
      (is (some #{"fluree-identity"} handler/fluree-request-header-keys))
      (is (some #{"fluree-policy"} handler/fluree-request-header-keys))
      (is (some #{"fluree-ledger"} handler/fluree-request-header-keys)))

    (testing "fluree-response-header-keys is defined"
      (is (seq handler/fluree-response-header-keys)))))

(deftest test-wrap-cors-composition
  (testing "wrap-cors function properly composes headers from constants"
    (let [test-handler (fn [_] {:status 200 :body "OK"})
          wrapped-handler (handler/wrap-cors nil test-handler)
          ;; Call with a basic request to verify the middleware doesn't error
          request {:request-method :get :uri "/test"}
          response (wrapped-handler request)]
      (is (= 200 (:status response)))
      (is (= "OK" (:body response))))))