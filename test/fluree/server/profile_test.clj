(ns fluree.server.profile-test
  (:require [clojure.test :refer [deftest is testing]]
            [fluree.db.util.json :as json]
            [fluree.server.config :as config]))

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
                    (vals parsed-with-profile))))))))