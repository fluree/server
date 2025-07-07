(ns fluree.server.config-substitution-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [fluree.db.connection.system :as conn-system]
            [fluree.server.system :as system]))

(def test-properties
  ["fluree.test.cache.size"
   "fluree.test.parallelism"
   "fluree.test.required.prop"])

(defn clear-test-properties
  "Test fixture that clears Java system properties before and after each test."
  [f]
  (doseq [prop test-properties]
    (System/clearProperty prop))
  (f)
  (doseq [prop test-properties]
    (System/clearProperty prop)))

(use-fixtures :each clear-test-properties)

(defn- minimal-config
  "Creates a minimal server configuration for testing ConfigurationValue substitution."
  [cache-config]
  {"@context" {"@base" "https://ns.flur.ee/config/memory/"
               "@vocab" "https://ns.flur.ee/system#"}
   "@id" "memoryServer"
   "@graph" [{"@id" "inMemoryStorage"
              "@type" "Storage"}
             {"@id" "connection"
              "@type" "Connection"
              "parallelism" 4
              "cacheMaxMb" cache-config
              "commitStorage" {"@id" "inMemoryStorage"}
              "indexStorage" {"@id" "inMemoryStorage"}
              "primaryPublisher" {"@type" "Publisher"
                                  "storage" {"@id" "inMemoryStorage"}}}
             {"@id" "consensus"
              "@type" "Consensus"
              "consensusProtocol" "standalone"
              "connection" {"@id" "connection"}}
             {"@id" "http"
              "@type" "API"
              "httpPort" 58090
              "maxTxnWaitMs" 120000}
             {"@id" "watcher"
              "@type" "Watcher"
              "connection" {"@id" "connection"}}]})

(defn- resolve-config-value
  "Directly resolves a ConfigurationValue using the same logic as the fluree/db system."
  [config-value-map]
  (let [env-var (get config-value-map "envVar")
        java-prop (get config-value-map "javaProp")
        default-val (get config-value-map "defaultVal")]
    (conn-system/get-priority-value env-var java-prop default-val)))

(defn- config-value-throws?
  "Tests if a ConfigurationValue throws an exception during resolution."
  [config-value-map]
  (let [config (minimal-config config-value-map)]
    (try
      (let [sys (system/start-config config)]
        (system/stop sys)
        false)
      (catch Exception _
        true))))

(deftest config-substitution-test
  (testing "Java property substitution"
    (System/setProperty "fluree.test.cache.size" "750")
    (let [config-value {"@type" "ConfigurationValue"
                        "javaProp" "fluree.test.cache.size"
                        "defaultVal" 200}]
      (is (= "750" (resolve-config-value config-value))
          "Should resolve to Java property value of 750")))

  (testing "Default value when property not set"
    (System/clearProperty "fluree.test.cache.size") ; Ensure property is not set
    (let [config-value {"@type" "ConfigurationValue"
                        "javaProp" "fluree.test.cache.size"
                        "defaultVal" 200}]
      (is (= 200 (resolve-config-value config-value))
          "Should resolve to default value of 200")))

  (testing "Resolution priority: Java property over environment variable"
    (System/setProperty "fluree.test.cache.size" "999")
    (let [config-value {"@type" "ConfigurationValue"
                        "javaProp" "fluree.test.cache.size"
                        "envVar" "PATH"
                        "defaultVal" 200}]
      (is (= "999" (resolve-config-value config-value))
          "Should resolve to Java property value of 999, not PATH or default")))

  (testing "Missing required value throws exception"
    (is (config-value-throws? {"@type" "ConfigurationValue"
                               "javaProp" "fluree.test.required.prop"})
        "Should throw when no value available and no default")))