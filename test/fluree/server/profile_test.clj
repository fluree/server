(ns fluree.server.profile-test
  (:require [clojure.test :refer [deftest is testing]]
            [fluree.db.util.json :as json]
            [fluree.server :as server]
            [fluree.server.command :as command]
            [fluree.server.config :as config]
            [fluree.server.system :as system]))

(def test-config-str
  "{
    \"@context\": {
      \"@base\": \"https://ns.flur.ee/config/test/\",
      \"@vocab\": \"https://ns.flur.ee/system#\",
      \"profiles\": {
        \"@container\": [\"@graph\", \"@index\"]
      }
    },
    \"@id\": \"testServer\",
    \"@graph\": [
      {
        \"@id\": \"localStorage\",
        \"@type\": \"Storage\",
        \"filePath\": \"/opt/fluree-server/data\"
      },
      {
        \"@id\": \"connection\",
        \"@type\": \"Connection\",
        \"parallelism\": 4,
        \"cacheMaxMb\": 1000,
        \"commitStorage\": {
          \"@id\": \"localStorage\"
        },
        \"indexStorage\": {
          \"@id\": \"localStorage\"
        }
      },
      {
        \"@id\": \"consensus\",
        \"@type\": \"Consensus\",
        \"consensusProtocol\": \"standalone\",
        \"maxPendingTxns\": 512,
        \"connection\": {
          \"@id\": \"connection\"
        }
      },
      {
        \"@id\": \"http\",
        \"@type\": \"API\",
        \"httpPort\": 8090
      }
    ],
    \"profiles\": {
      \"dev\": [
        {
          \"@id\": \"localStorage\",
          \"filePath\": \"dev/data\"
        },
        {
          \"@id\": \"connection\",
          \"cacheMaxMb\": 200
        },
        {
          \"@id\": \"consensus\",
          \"maxPendingTxns\": 16
        }
      ]
    }
  }")

(deftest profile-merge-test
  (testing "Profile configuration is properly merged when starting with a profile"
    ;; We need to implement profile support in start-config
    ;; For now, let's create a test that will fail until we implement it

    (let [json-config (json/parse test-config-str false)
          ;; When we call start-config with "dev" profile
          ;; We expect the profile to be merged
          ]

      ;; Test that without profile, we get default values
      (testing "Without profile, default values are used"
        (let [parsed-no-profile (config/parse json-config)]
          ;; Find the localStorage node
          (is (some #(and (= :https<cl><sl><sl>ns<do>flur<do>ee<sl>config<sl>test<sl>/localStorage (:id %))
                          (= "/opt/fluree-server/data"
                             (-> (get % "https://ns.flur.ee/system#filePath")
                                 first
                                 :value)))
                    (vals parsed-no-profile)))

          ;; Find the connection node
          (is (some #(and (= :https<cl><sl><sl>ns<do>flur<do>ee<sl>config<sl>test<sl>/connection (:id %))
                          (= 1000
                             (-> (get % "https://ns.flur.ee/system#cacheMaxMb")
                                 first
                                 :value)))
                    (vals parsed-no-profile)))

          ;; Find the consensus node
          (is (some #(and (= :https<cl><sl><sl>ns<do>flur<do>ee<sl>config<sl>test<sl>/consensus (:id %))
                          (= 512
                             (-> (get % "https://ns.flur.ee/system#maxPendingTxns")
                                 first
                                 :value)))
                    (vals parsed-no-profile)))))

      ;; Test with dev profile
      (testing "With dev profile, values are overridden"
        (let [config-with-profile (config/apply-profile json-config "dev")
              parsed-with-profile (config/parse config-with-profile)]

          ;; Find the localStorage node - should have dev/data
          (is (some #(and (= :https<cl><sl><sl>ns<do>flur<do>ee<sl>config<sl>test<sl>/localStorage (:id %))
                          (= "dev/data"
                             (-> (get % "https://ns.flur.ee/system#filePath")
                                 first
                                 :value)))
                    (vals parsed-with-profile)))

          ;; Find the connection node - cacheMaxMb should be 200
          (is (some #(and (= :https<cl><sl><sl>ns<do>flur<do>ee<sl>config<sl>test<sl>/connection (:id %))
                          (= 200
                             (-> (get % "https://ns.flur.ee/system#cacheMaxMb")
                                 first
                                 :value)))
                    (vals parsed-with-profile)))

          ;; Find the consensus node - maxPendingTxns should be 16
          (is (some #(and (= :https<cl><sl><sl>ns<do>flur<do>ee<sl>config<sl>test<sl>/consensus (:id %))
                          (= 16
                             (-> (get % "https://ns.flur.ee/system#maxPendingTxns")
                                 first
                                 :value)))
                    (vals parsed-with-profile)))))))

  (deftest error-handling-test
    (testing "Error conditions are handled gracefully"

      (testing "Invalid JSON throws helpful error"
        (is (thrown-with-msg?
             Exception
             #"Unexpected character"
             (system/start-config "{invalid json" nil))))

      (testing "Invalid JSON with profile throws helpful error"
        (is (thrown-with-msg?
             Exception
             #"Unexpected character"
             (system/start-config "{\"@context\": bad json}" "dev"))))

      (testing "Non-existent config file throws helpful error"
        (is (thrown-with-msg?
             clojure.lang.ExceptionInfo
             #"Unable to load configuration file"
             (system/start-file "/non/existent/path/config.json" nil))))

      (testing "Non-existent resource throws helpful error"
        (is (thrown-with-msg?
             clojure.lang.ExceptionInfo
             #"Unable to load configuration resource"
             (system/start-resource "non-existent-resource.json" nil))))

      (testing "Profile with malformed structure handles gracefully"
        (let [config-with-bad-profile
              {"@context" {"@base" "https://ns.flur.ee/config/test/"
                           "@vocab" "https://ns.flur.ee/system#"}
               "@id" "testConfig"
               "@graph" [{"@id" "node1" "@type" "Storage"}]
               "profiles" {"bad" "not-an-array"}}]
        ;; Should not throw, but should log warning
          (is (= (dissoc config-with-bad-profile "profiles")
                 (config/apply-profile config-with-bad-profile "bad")))))

      (testing "Profile override missing @id is skipped"
        (let [config-with-missing-id
              {"@context" {"@base" "https://ns.flur.ee/config/test/"
                           "@vocab" "https://ns.flur.ee/system#"}
               "@id" "testConfig"
               "@graph" [{"@id" "storage"
                          "@type" "Storage"
                          "filePath" "/prod/data"}]
               "profiles" {"dev" [{"filePath" "/dev/data"}  ;; Missing @id
                                  {"@id" "storage"
                                   "filePath" "/dev2/data"}]}}
              result (config/apply-profile config-with-missing-id "dev")]
        ;; Should apply the override that has @id, skip the one without
          (is (= "/dev2/data"
                 (get-in result ["@graph" 0 "filePath"])))))

      (testing "Empty profile array works"
        (let [config-with-empty-profile
              {"@context" {"@base" "https://ns.flur.ee/config/test/"
                           "@vocab" "https://ns.flur.ee/system#"}
               "@id" "testConfig"
               "@graph" [{"@id" "storage" "@type" "Storage"}]
               "profiles" {"empty" []}}
              result (config/apply-profile config-with-empty-profile "empty")]
        ;; Should return config without profiles
          (is (= (dissoc config-with-empty-profile "profiles")
                 result))))

      (testing "Profile with null values works"
        (let [config-with-null
              {"@context" {"@base" "https://ns.flur.ee/config/test/"
                           "@vocab" "https://ns.flur.ee/system#"}
               "@id" "testConfig"
               "@graph" [{"@id" "storage"
                          "@type" "Storage"
                          "filePath" "/prod/data"
                          "maxSize" 1000}]
               "profiles" {"dev" [{"@id" "storage"
                                   "filePath" "/dev/data"
                                   "maxSize" nil}]}}
              result (config/apply-profile config-with-null "dev")]
        ;; null should override the value
          (is (= "/dev/data"
                 (get-in result ["@graph" 0 "filePath"])))
          (is (nil? (get-in result ["@graph" 0 "maxSize"]))))))

    (testing "Valid JSON but invalid config structure throws error"
      (is (thrown?
           Exception
           (system/start-config "{\"valid\": \"json\", \"but\": \"not valid config\"}" nil))))

    (testing "Config missing required fields throws error"
      (is (thrown?
           Exception
           (system/start-config (json/stringify {"@context" {"@base" "test"}
                                                 "@graph" [{"@id" "broken"}]}) nil))))))

(deftest cli-argument-test
  (testing "Command-line argument parsing and server startup"

    (testing "Profile argument is parsed correctly"
      (let [parsed (command/parse ["--profile" "dev"])]
        (is (= "dev" (get-in parsed [:options :profile])))
        (is (empty? (:errors parsed)))))

    (testing "Config file argument is parsed correctly"
      (let [parsed (command/parse ["--config" "/path/to/config.json"])]
        (is (= "/path/to/config.json" (get-in parsed [:options :config])))
        (is (empty? (:errors parsed)))))

    (testing "String config argument is parsed correctly"
      (let [parsed (command/parse ["--string" "{\"test\": \"config\"}"])]
        (is (= "{\"test\": \"config\"}" (get-in parsed [:options :string])))
        (is (empty? (:errors parsed)))))

    (testing "Resource argument is parsed correctly"
      (let [parsed (command/parse ["--resource" "test-config.jsonld"])]
        (is (= "test-config.jsonld" (get-in parsed [:options :resource])))
        (is (empty? (:errors parsed)))))

    (testing "Multiple config options produce error"
      (let [parsed (command/parse ["--config" "/path/config.json" "--string" "{\"test\": true}"])]
        (is (seq (:errors parsed)))
        (is (some #(re-find #"single configuration option" %) (:errors parsed)))))

    (testing "Profile with config file"
      (let [parsed (command/parse ["--profile" "dev" "--config" "/path/to/config.json"])]
        (is (= "dev" (get-in parsed [:options :profile])))
        (is (= "/path/to/config.json" (get-in parsed [:options :config])))
        (is (empty? (:errors parsed)))))

    (testing "Help option"
      (let [parsed (command/parse ["--help"])]
        (is (true? (get-in parsed [:options :help])))
        (is (empty? (:errors parsed)))))

    (testing "Unknown options are captured"
      (let [parsed (command/parse ["--unknown" "option" "extra"])]
        (is (= ["option" "extra"] (:arguments parsed)))))

    (testing "Server start with string config and profile"
      (let [config-with-profile {"@context" {"@base" "https://ns.flur.ee/config/test/"
                                             "@vocab" "https://ns.flur.ee/system#"
                                             "profiles" {"@container" ["@graph" "@index"]}}
                                 "@id" "test"
                                 "@graph" [{"@id" "storage"
                                            "@type" "Storage"
                                            "filePath" "/prod/data"}]
                                 "profiles" {"dev" [{"@id" "storage"
                                                     "filePath" "/dev/data"}]}}
            parsed-args (command/parse ["--string" (json/stringify config-with-profile)
                                        "--profile" "dev"])]
        ;; Test that the server would start with these args
        (is (= "dev" (get-in parsed-args [:options :profile])))
        (is (string? (get-in parsed-args [:options :string])))
        ;; Verify the config string contains our test data
        (let [parsed-config (json/parse (get-in parsed-args [:options :string]) false)]
          (is (= "/prod/data" (get-in parsed-config ["@graph" 0 "filePath"])))
          (is (= "/dev/data" (get-in parsed-config ["profiles" "dev" 0 "filePath"]))))))

    (testing "Server start simulation with profile"
      ;; Create a minimal valid config
      (let [valid-config {"@context" {"@base" "https://ns.flur.ee/config/test/"
                                      "@vocab" "https://ns.flur.ee/system#"}
                          "@id" "testServer"
                          "@graph" [{"@id" "memoryStorage"
                                     "@type" "Storage"}
                                    {"@id" "connection"
                                     "@type" "Connection"
                                     "parallelism" 1
                                     "commitStorage" {"@id" "memoryStorage"}
                                     "indexStorage" {"@id" "memoryStorage"}}
                                    {"@id" "consensus"
                                     "@type" "Consensus"
                                     "consensusProtocol" "standalone"
                                     "connection" {"@id" "connection"}}]
                          "profiles" {"test" [{"@id" "connection"
                                               "parallelism" 2}]}}
            config-str (json/stringify valid-config)
            parsed-args (command/parse ["--string" config-str "--profile" "test"])]

        ;; Instead of starting the full server, test the configuration flow
        (is (empty? (:errors parsed-args)))
        (is (= "test" (get-in parsed-args [:options :profile])))

        ;; Test that the config would be processed correctly
        (let [json-config (json/parse config-str false)
              config-with-profile (config/apply-profile json-config "test")]
          ;; Verify profile was applied
          (is (= 2 (get-in config-with-profile ["@graph" 1 "parallelism"])))
          ;; Verify profiles section is removed after application
          (is (nil? (get config-with-profile "profiles"))))))

    (testing "Error handling for invalid arguments"
      (testing "Invalid JSON string config"
        (let [parsed-args (command/parse ["--string" "{invalid json}"])]
          (is (empty? (:errors parsed-args))) ; Parsing args succeeds
          (is (thrown-with-msg?
               Exception
               #"Unexpected character"
               (server/start parsed-args)))))

      (testing "Non-existent config file"
        (let [parsed-args (command/parse ["--config" "/non/existent/file.json"])]
          (is (empty? (:errors parsed-args))) ; Parsing args succeeds
          (is (thrown-with-msg?
               clojure.lang.ExceptionInfo
               #"Unable to load configuration file"
               (server/start parsed-args)))))

      (testing "Non-existent resource"
        (let [parsed-args (command/parse ["--resource" "non-existent.jsonld"])]
          (is (empty? (:errors parsed-args))) ; Parsing args succeeds
          (is (thrown-with-msg?
               clojure.lang.ExceptionInfo
               #"Unable to load configuration resource"
               (server/start parsed-args)))))

      (testing "Profile with default config (file-config.jsonld)"
        (let [parsed-args (command/parse ["--profile" "dev"])]
          (is (empty? (:errors parsed-args)))
          ;; This should work since file-config.jsonld has a dev profile
          (let [system (server/start parsed-args)]
            (is (map? system))
            ;; Clean up by stopping the server
            (system/stop system)))))

    (testing "Short option forms"
      (let [parsed (command/parse ["-p" "dev" "-c" "/path/to/config.json"])]
        (is (= "dev" (get-in parsed [:options :profile])))
        (is (= "/path/to/config.json" (get-in parsed [:options :config])))
        (is (empty? (:errors parsed)))))

    (testing "Complete command-line simulation"
      ;; Simulate: clojure -M -m fluree.server --profile dev --string '{"@context": {...}}'
      (let [config {"@context" {"@base" "https://ns.flur.ee/config/test/"
                                "@vocab" "https://ns.flur.ee/system#"}
                    "@id" "cliTest"
                    "@graph" [{"@id" "storage"
                               "@type" "Storage"
                               "filePath" "/prod/data"}
                              {"@id" "connection"
                               "@type" "Connection"
                               "parallelism" 4
                               "commitStorage" {"@id" "storage"}
                               "indexStorage" {"@id" "storage"}}
                              {"@id" "consensus"
                               "@type" "Consensus"
                               "consensusProtocol" "standalone"
                               "connection" {"@id" "connection"}}]
                    "profiles" {"dev" [{"@id" "storage"
                                        "filePath" "/dev/data"}
                                       {"@id" "connection"
                                        "parallelism" 2}]}}
            config-str (json/stringify config)
            ;; Simulate command line args
            args ["--profile" "dev" "--string" config-str]
            parsed (command/parse args)]

        ;; Verify parsing succeeds
        (is (empty? (:errors parsed)))
        (is (= "dev" (get-in parsed [:options :profile])))

        ;; Test the full flow without actually starting the server
        (let [json-config (json/parse (get-in parsed [:options :string]) false)
              applied-config (config/apply-profile json-config "dev")]
          ;; Verify dev profile was applied
          (is (= "/dev/data" (get-in applied-config ["@graph" 0 "filePath"])))
          (is (= 2 (get-in applied-config ["@graph" 1 "parallelism"])))
          ;; Verify profiles section removed
          (is (nil? (get applied-config "profiles"))))))))
