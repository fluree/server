(ns fluree.server.handlers.remote-resource
  (:require [fluree.db.api :as-alias fluree]
            [fluree.db.connection :as connection]
            [fluree.db.util.async :refer [<??]]
            [fluree.db.util.json :as json]
            [fluree.db.util.log :as log]
            [fluree.server.handlers.shared :refer [defhandler]]))

(defhandler latest-commit
  [{:keys [fluree/conn]
    {{ledger-address :resource :as body} :body} :parameters}]
  (log/debug "Latest commit lookup request:" body)
  (let [result (<?? (connection/read-publisher-commit conn ledger-address))]
    {:status 200
     :body   result}))

(defhandler read-resource-address
  [{:keys [fluree/conn]
    {{resource-address :resource :as body} :body} :parameters}]
  (log/debug "Remote resource read request:" body)
  (let [result (<?? (connection/read-file-address conn resource-address))]
    {:status 200
     :body   result}))

(defhandler parse-address-hash
  [{:keys [fluree/conn]
    {{resource-address :address :as body} :body} :parameters}]
  (log/debug "Remote resource parse address hash request:" body)
  (let [result (<?? (connection/parse-address-hash conn resource-address))]
    {:status 200
     :body   (json/stringify result)}))

(defhandler published-ledger-addresses
  [{:keys [fluree/conn]
    {{:keys [ledger] :as body} :body} :parameters}]
  (log/debug "Retrieve ledger address request:" body)
  (let [result (<?? (connection/published-addresses conn ledger))]
    {:status 200
     :body   {:addresses result}}))
