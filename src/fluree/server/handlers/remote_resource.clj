(ns fluree.server.handlers.remote-resource
  (:require [clojure.string :as str]
            [fluree.db.conn.proto :as conn-proto]
            [fluree.db.json-ld.api :as fluree]
            [fluree.db.util.async :refer [<? <?? go-try]]
            [fluree.db.util.log :as log]
            [fluree.server.handlers.ledger :refer [error-catching-handler deref!]]))


(defn read-latest-commit
  [conn resource-name]
  (go-try
    (let [commit-addr (<? (conn-proto/-lookup conn resource-name))
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

(def read-handler
  (error-catching-handler
    (fn [{:keys [fluree/conn credential/did] {{resource-name :resource :as params} :body} :parameters :as parameters}]
      (log/debug "Remote resource read request params:" params)
      (let [resource-address (resource-address conn resource-name)
            file-read?       (str/ends-with? resource-address ".json")
            result           (if file-read?
                               (<?? (conn-proto/-c-read conn resource-address))
                               (<?? (read-latest-commit conn resource-address)))]
        {:status 200
         :body   result}))))
