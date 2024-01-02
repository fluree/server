(ns http-calls
  (:require [clj-http.client :as client]
            [fluree.db.util.json :as json]
            [fluree.server.consensus.network.multi-addr :refer [multi->map]]
            [user :as user]))

(defn server-env->http-address
  [server-env]
  (let [{:keys [http/server fluree/consensus]} server-env
        http-port (get server :port)
        host      (-> consensus :consensus-this-server multi->map :host)]
    (str "http://" host ":" http-port "/")))

(def ledger-name "my/test")

(def server-1-address (server-env->http-address user/server-1-env))
(def server-2-address (server-env->http-address user/server-2-env))
(def server-3-address (server-env->http-address user/server-3-env))


(comment

  (-> (client/get (str server-1-address "swagger.json"))
      :body
      (json/parse false))

  (-> (client/post (str server-1-address "fluree/create")
                   {:body               (json/stringify-UTF8
                                          {"@context" ["https://ns.flur.ee" {"ex" "http://example.org/"}]
                                           "ledger"   ledger-name
                                           "insert"   {"id"      "ex:test1"
                                                       "ex:name" "Brian1"}})
                    ;:headers {"X-Api-Version" "2"}
                    :content-type       :json
                    :socket-timeout     1000 ;; in milliseconds
                    :connection-timeout 1000 ;; in milliseconds
                    :accept             :json}))

  (-> (client/post (str server-1-address "fluree/transact")
                   {:body               (json/stringify-UTF8
                                          {"@context" ["https://ns.flur.ee" {"ex" "http://example.org/"}]
                                           "ledger"   ledger-name
                                           "insert"   {"id"      "ex:test2"
                                                       "ex:name" "Brian2"}})
                    ;:headers {"X-Api-Version" "2"}
                    :content-type       :json
                    :socket-timeout     1000 ;; in milliseconds
                    :connection-timeout 1000 ;; in milliseconds
                    :accept             :json}))

  (-> (client/post (str server-1-address "fluree/query")
                   {:body               (json/stringify-UTF8
                                          {"select" {"?s" ["*"]}
                                           "from"   ledger-name
                                           "where"  {"@id"     "?s"
                                                     "ex:name" "?x"}})
                    ;:headers {"X-Api-Version" "2"}
                    :content-type       :json
                    :socket-timeout     1000 ;; in milliseconds
                    :connection-timeout 1000 ;; in milliseconds
                    :accept             :json})
      :body
      (json/parse false))

  )