(ns fluree.server.handlers.remote-resource
  (:require [clojure.string :as str]
            [fluree.db.conn.proto :as conn-proto]
            [fluree.db.json-ld.api :as-alias fluree]
            [fluree.db.nameservice.core :as nameservice]
            [fluree.db.util.async :refer [<? <?? go-try]]
            [fluree.db.util.log :as log]
            [fluree.server.handlers.shared :refer [defhandler]]))

(defn read-latest-commit
  [conn resource-name]
  (go-try
    (let [commit-addr (<? (nameservice/lookup-commit conn resource-name nil))
          _           (when-not commit-addr
                        (throw (ex-info (str "Unable to load. No commit exists for: " alias)
                                        {:status 400 :error :db/invalid-commit-address})))
          commit-data (<? (conn-proto/-c-read conn commit-addr))]
      (assoc commit-data "address" commit-addr))))

(defn resource-address
  [conn resource-name]
  (if (str/starts-with? resource-name "fluree:")
    resource-name
    (str "fluree:" (name (conn-proto/-method conn)) "://" resource-name)))

(defhandler read-handler
  [{:keys [fluree/conn]
    {{resource-name :resource :as body} :body} :parameters}]
  (log/debug "Remote resource read request:" body)
  (let [resource-address (resource-address conn resource-name)
        file-read?       (str/ends-with? resource-address ".json")
        result           (if file-read?
                           (<?? (conn-proto/-c-read conn resource-address))
                           (<?? (read-latest-commit conn resource-address)))]
    {:status 200
     :body   result}))
