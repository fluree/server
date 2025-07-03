(ns fluree.server.config-substitution-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [fluree.server.system :as system]
            [integrant.core :as ig]))

(defn clear-test-properties
  "Test fixture that ensures Java system properties used in tests are cleared
   before and after each test runs. This prevents test pollution where properties
   set in one test could affect another test's behavior."
  [f]
  (doseq [prop ["fluree.test.http.port"
                "fluree.test.cache.size"]]
    (System/clearProperty prop))
  (f)
  (doseq [prop ["fluree.test.http.port"
                "fluree.test.cache.size"]]
    (System/clearProperty prop)))

(use-fixtures :each clear-test-properties)

(defn get-resolved-config-values
  "Extract all resolved ConfigurationValue entries from the system map.
   These are stored with generated keys in the '_<cl>' namespace."
  [system-map]
  (filter (fn [[k v]]
            (and (keyword? k)
                 (= "_<cl>" (namespace k))
                 (map? v)
                 (contains? v :value)))
          system-map))

(defn find-resolved-value
  "Find a specific resolved value in the system map.
   Returns the value as a string, since ConfigurationValues are stored as strings."
  [system-map target-value]
  (let [resolved-values (get-resolved-config-values system-map)
        str-value (str target-value)]
    (some (fn [[_k v]]
            (when (= str-value (:value v))
              (:value v)))
          resolved-values)))

(defn minimal-test-config
  "Create a minimal config for testing with a single ConfigurationValue"
  [config-value-map]
  {"@context" {"@base" "https://ns.flur.ee/test/"
               "@vocab" "https://ns.flur.ee/system#"}
   "@id" "testServer"
   "@graph" [{"@id" "memoryStorage"
              "@type" "Storage"}
             {"@id" "connection"
              "@type" "Connection"
              "parallelism" 4
              "cacheMaxMb" (if (map? config-value-map)
                             config-value-map
                             100)
              "commitStorage" {"@id" "memoryStorage"}
              "indexStorage" {"@id" "memoryStorage"}}
             {"@id" "consensus"
              "@type" "Consensus"
              "consensusProtocol" "standalone"
              "connection" {"@id" "connection"}}
             {"@id" "http"
              "@type" "API"
              "httpPort" 8090}]})

(deftest config-substitution-test
  (testing "ConfigurationValue functionality"

    (testing "Java property substitution"
      ;; Set a Java system property
      (System/setProperty "fluree.test.cache.size" "750")

      (let [config (minimal-test-config {"@type" "ConfigurationValue"
                                         "javaProp" "fluree.test.cache.size"
                                         "defaultVal" 200})
            system-map (system/start-config config)]

        (try
          (is (some? system-map)
              "System should start successfully with ConfigurationValue")

          ;; Verify the Java property value was used
          (let [resolved-value (find-resolved-value system-map 750)]
            (is (= "750" resolved-value)
                "ConfigurationValue should resolve to Java property value"))

          (finally
            (when system-map
              (ig/halt! system-map))))))

    (testing "Default value when property not set"
      ;; Ensure property is not set
      (System/clearProperty "fluree.test.cache.size")

      (let [config (minimal-test-config {"@type" "ConfigurationValue"
                                         "javaProp" "fluree.test.cache.size"
                                         "defaultVal" 200})
            system-map (system/start-config config)]

        (try
          (is (some? system-map)
              "System should start with default value")

          ;; Verify the default value was used
          ;; Note: Numeric defaults are stored as numbers, string properties as strings
          (let [resolved-values (get-resolved-config-values system-map)]
            (is (= 1 (count resolved-values))
                "Should have one resolved ConfigurationValue")
            (is (some #(= 200 (:value (second %))) resolved-values)
                "ConfigurationValue should use default value 200"))

          (finally
            (when system-map
              (ig/halt! system-map))))))

    (testing "Environment variable substitution"
      ;; Note: We can't easily set environment variables in tests,
      ;; but we can test with existing ones like PATH or USER
      (let [config (minimal-test-config {"@type" "ConfigurationValue"
                                         "envVar" "PATH"
                                         "defaultVal" "/default/path"})
            system-map (system/start-config config)]

        (try
          (is (some? system-map)
              "System should start with environment variable")

          ;; Verify that we got a value (either from env var or default)
          (let [resolved-values (get-resolved-config-values system-map)]
            (is (pos? (count resolved-values))
                "Should have resolved ConfigurationValue")
            ;; We can't predict the exact PATH value, but it should exist
            (is (some #(contains? (second %) :value) resolved-values)
                "Should have a resolved value"))

          (finally
            (when system-map
              (ig/halt! system-map))))))

    (testing "Priority: Java property over environment variable"
      ;; Set both Java property and use an env var that exists
      (System/setProperty "fluree.test.cache.size" "999")

      (let [config (minimal-test-config {"@type" "ConfigurationValue"
                                         "javaProp" "fluree.test.cache.size"
                                         "envVar" "PATH"
                                         "defaultVal" 200})
            system-map (system/start-config config)]

        (try
          (is (some? system-map)
              "System should start with both javaProp and envVar")

          ;; Verify Java property takes precedence
          (let [resolved-value (find-resolved-value system-map 999)]
            (is (= "999" resolved-value)
                "Java property should take precedence over environment variable"))

          (finally
            (when system-map
              (ig/halt! system-map))))))

    (testing "Error when no value available"
      ;; Clear property to ensure no value
      (System/clearProperty "fluree.test.required.prop")

      (let [config (minimal-test-config {"@type" "ConfigurationValue"
                                         "javaProp" "fluree.test.required.prop"})]

        (is (thrown? Exception
                     (system/start-config config))
            "Should throw exception when ConfigurationValue has no value available")))

    (testing "Multiple ConfigurationValues in same config"
      ;; Set different properties
      (System/setProperty "fluree.test.http.port" "9090")
      (System/setProperty "fluree.test.cache.size" "512")

      (let [config {"@context" {"@base" "https://ns.flur.ee/test/"
                                "@vocab" "https://ns.flur.ee/system#"}
                    "@id" "testServer"
                    "@graph" [{"@id" "memoryStorage"
                               "@type" "Storage"}
                              {"@id" "connection"
                               "@type" "Connection"
                               "parallelism" 4
                               "cacheMaxMb" {"@type" "ConfigurationValue"
                                             "javaProp" "fluree.test.cache.size"
                                             "defaultVal" 200}
                               "commitStorage" {"@id" "memoryStorage"}
                               "indexStorage" {"@id" "memoryStorage"}}
                              {"@id" "consensus"
                               "@type" "Consensus"
                               "consensusProtocol" "standalone"
                               "connection" {"@id" "connection"}}
                              {"@id" "http"
                               "@type" "API"
                               "httpPort" {"@type" "ConfigurationValue"
                                           "javaProp" "fluree.test.http.port"
                                           "defaultVal" 8090}}]}
            system-map (system/start-config config)]

        (try
          (is (some? system-map)
              "System should start with multiple ConfigurationValues")

          ;; Verify both values were resolved
          (let [resolved-values (get-resolved-config-values system-map)]
            (is (= 2 (count resolved-values))
                "Should have exactly 2 resolved ConfigurationValues")

            (is (= "9090" (find-resolved-value system-map 9090))
                "HTTP port should be resolved")
            (is (= "512" (find-resolved-value system-map 512))
                "Cache size should be resolved"))

          (finally
            (when system-map
              (ig/halt! system-map))))))))