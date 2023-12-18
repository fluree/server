(ns fluree.server.integration.test-system
  (:require [clj-http.client :as http]
            [donut.system :as ds]
            [fluree.db.util.json :as json]
            [fluree.server.main :as sys])
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

(defn find-open-port
  ([] (find-open-port nil))
  ([_] ; so it can be used in swap!
   (let [socket (ServerSocket. 0)]
     (.close socket)
     (.getLocalPort socket))))

(defn find-host-address
  ([] (find-open-port nil))
  ([_] ; so it can be used in swap!
   (let [socket (ServerSocket. 0)]
     (.close socket)
     (-> socket
         (.getInetAddress)
         (.getHostAddress)))))

(defonce api-port (atom nil))
(defonce host-address (atom nil))
(defonce consensus-port-1 (atom nil))
(defonce consensus-port-2 (atom nil))
(defonce consensus-port-3 (atom nil))

(defn set-server-ports
  []
  (swap! api-port find-open-port)
  (swap! consensus-port-1 find-open-port)
  (swap! consensus-port-2 find-open-port)
  (swap! consensus-port-3 find-open-port))

(def default-context
  {"id"     "@id"
   "type"   "@type"
   "graph"  "@graph"
   "ex"     "http://example.com/"
   "schema" "http://schema.org/"
   "rdf"    "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
   "f"      "https://ns.flur.ee/ledger#"})

(defmethod ds/named-system :test
  [_]
  (let [multi-addr-1 (str "/ip4/127.0.0.1/tcp/" @consensus-port-1)]
    (ds/system :dev {[:env] {:http/server {:port           @api-port
                                           :max-tx-wait-ms 45000}
                             :fluree/connection
                             {:method       :memory
                              :parallelism  1
                              :cache-max-mb 100}
                             :fluree/consensus
                             {:consensus-type        :raft
                              :consensus-servers     multi-addr-1
                              :consensus-this-server multi-addr-1}}})))

(defn run-test-server
  [run-tests]
  (set-server-ports)
  (let [stop-server (sys/run-server {:profile :test})]
    (run-tests)
    (stop-server)))

(defn api-url [endpoint]
  (str "http://localhost:" @api-port "/fluree/" (name endpoint)))

(defn api-post [endpoint req]
  (http/post (api-url endpoint) (assoc req :throw-exceptions false)))

(defn api-get [endpoint req]
  (http/get (api-url endpoint) (assoc req :throw-exceptions false)))

(defn create-rand-ledger
  [name-root]
  (let [ledger-name (str name-root "-" (random-uuid))
        req         (json/stringify
                     {"ledger"   ledger-name
                      "@context" ["https://ns.flur.ee"
                                  default-context
                                  {"foo" "http://foobar.com/"}]
                      "insert"   [{"id"       "foo:create-test"
                                   "type"     "foo:test"
                                   "foo:name" "create-endpoint-test"}]})
        res         (update (api-post :create {:body req :headers json-headers})
                            :body json/parse)]
    (if (= 201 (:status res))
      (get-in res [:body :ledger])
      (throw (ex-info "Error creating random ledger" res)))))

(def auth
  {:id      "did:fluree:TfHgFTQQiJMHaK1r1qxVPZ3Ridj9pCozqnh"
   :public  "03b160698617e3b4cd621afd96c0591e33824cb9753ab2f1dace567884b4e242b0"
   :private "509553eece84d5a410f1012e8e19e84e938f226aa3ad144e2d12f36df0f51c1e"})
