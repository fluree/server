(ns fluree.server.integration.test-system
  (:require [clj-http.client :as http]
            [fluree.db.util.json :as json]
            [fluree.server.config :as-alias config]
            [fluree.server.system :as system])
  (:import (java.net ServerSocket)))

(def json-headers
  {"Content-Type" "application/json"
   "Accept"       "application/json"})

(def edn-headers
  {"Content-Type" "application/edn"
   "Accept"       "application/edn"})

(def sparql-headers
  {"Content-Type" "application/sparql-query"
   "Accept"       "application/json"})

(def jwt-headers
  {"Content-Type" "application/jwt"
   "Accept"       "application/json"})

(defn get-socket-port
  [^ServerSocket s]
  (.getLocalPort s))

(defn close-socket
  [^ServerSocket s]
  (.close s))

(defn find-open-ports
  [n]
  (let [sockets (repeatedly n #(ServerSocket. 0))
        ports   (mapv get-socket-port sockets)]
    (->> sockets (map close-socket) dorun)
    ports))

(defonce api-port (atom nil))
(defonce consensus-port-1 (atom nil))
(defonce consensus-port-2 (atom nil))
(defonce consensus-port-3 (atom nil))

(defn set-server-ports
  []
  (let [ports (find-open-ports 4)]
    (reset! api-port (nth ports 0))
    (reset! consensus-port-1 (nth ports 1))
    (reset! consensus-port-2 (nth ports 2))
    (reset! consensus-port-3 (nth ports 3))))

(def default-context
  {"id"     "@id"
   "type"   "@type"
   "graph"  "@graph"
   "ex"     "http://example.com/"
   "schema" "http://schema.org/"
   "rdf"    "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
   "f"      "https://ns.flur.ee/ledger#"})

(defn run-test-server
  [run-tests]
  (set-server-ports)
  (let [config  {"@context" {"@base"    "https://ns.flur.ee/dev/config/",
                             "@vocab"   "https://ns.flur.ee/system#",
                             "profiles" {"@container" ["@graph", "@id"]}}
                 "@id"      "testSystem"
                 "@graph"   [{"@id"   "memoryStorage"
                              "@type" "Storage"}
                             {"@id"              "testConnection"
                              "@type"            "Connection"
                              "parallelism"      1
                              "cacheMaxMb"       100
                              "commitStorage"    {"@id" "memoryStorage"}
                              "indexStorage"     {"@id" "memoryStorage"}
                              "primaryPublisher" {"@type"   "Publisher"
                                                  "storage" {"@id" "memoryStorage"}}}
                             {"@id"               "testConsensus"
                              "@type"             "Consensus"
                              "consensusProtocol" "standalone"
                              "connection"        {"@id" "testConnection"}
                              "maxPendingTxns"    16}
                             {"@id"          "testApiServer"
                              "@type"        "API"
                              "httpPort"     @api-port
                              "maxTxnWaitMs" 45000}]}
        server (system/start-config config)]
    (run-tests)
    (system/stop server)))

(defn api-url
  ([endpoint]
   (api-url endpoint @api-port))
  ([endpoint port]
   (str "http://localhost:" port "/fluree/" (name endpoint))))

(defn api-post
  ([endpoint req]
   (api-post endpoint req @api-port))
  ([endpoint req port]
   (http/post (api-url endpoint port) (assoc req :throw-exceptions false))))

(defn api-get
  ([endpoint req]
   (api-get endpoint req @api-port))
  ([endpoint req port]
   (http/get (api-url endpoint port) (assoc req :throw-exceptions false))))

(defn create-rand-ledger
  ([name-root]
   (create-rand-ledger name-root @api-port))
  ([name-root port]
   (let [ledger-name (str name-root "-" (random-uuid))
         req         {"ledger"   ledger-name
                      "@context" ["https://ns.flur.ee"
                                  default-context
                                  {"foo" "http://foobar.com/"}]
                      "insert"   [{"id"       "foo:create-test"
                                   "type"     "foo:test"
                                   "foo:name" "create-endpoint-test"}]}
         res         (-> (api-post :create {:body (json/stringify req) :headers json-headers} port)
                         (update :body json/parse))]
     (if (= 201 (:status res))
       (get-in res [:body :ledger])
       (throw (ex-info "Error creating random ledger" res))))))

(def auth
  {:id      "did:fluree:TfHgFTQQiJMHaK1r1qxVPZ3Ridj9pCozqnh"
   :public  "03b160698617e3b4cd621afd96c0591e33824cb9753ab2f1dace567884b4e242b0"
   :private "509553eece84d5a410f1012e8e19e84e938f226aa3ad144e2d12f36df0f51c1e"})

(defn run-closed-test-server
  [run-tests]
  (set-server-ports)
  (let [config  {"@context" {"@base"    "https://ns.flur.ee/dev/config/",
                             "@vocab"   "https://ns.flur.ee/system#",
                             "profiles" {"@container" ["@graph", "@id"]}}
                 "@id"      "testSystem"
                 "@graph"   [{"@id"   "memoryStorage"
                              "@type" "Storage"}
                             {"@id"              "testConnection"
                              "@type"            "Connection"
                              "parallelism"      1
                              "cacheMaxMb"       100
                              "commitStorage"    {"@id" "memoryStorage"}
                              "indexStorage"     {"@id" "memoryStorage"}
                              "primaryPublisher" {"@type"   "Publisher"
                                                  "storage" {"@id" "memoryStorage"}}}
                             {"@id"               "testConsensus"
                              "@type"             "Consensus"
                              "consensusProtocol" "standalone"
                              "connection"        {"@id" "testConnection"}
                              "maxPendingTxns"    16}
                             {"@id"          "testApiServer"
                              "@type"        "API"
                              "httpPort"     @api-port
                              "closedMode"   true
                              "rootIdentities" [(:id auth)]
                              "maxTxnWaitMs" 45000}]}
        server  (system/start-config config)]
    (run-tests)
    (system/stop server)))
