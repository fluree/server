(ns fluree.server.handlers.remote-resource
  (:require [fluree.db.api :as-alias fluree]
            [fluree.db.connection :as connection]
            [fluree.db.util.async :refer [<??]]
            [fluree.db.util.log :as log]
            [fluree.server.handlers.shared :refer [defhandler]]))

(defhandler latest-commit
  [{:keys [fluree/conn]
    {{ledger-address :resource :as body} :body} :parameters}]
  (log/debug "Latest commit lookup request:" body)
  (let [result (<?? (connection/read-latest-local-commit conn ledger-address))]
    {:status 200
     :body   result}))

(defhandler read-resource-address
  [{:keys [fluree/conn]
    {{resource-address :resource :as body} :body} :parameters}]
  (log/debug "Remote resource read request:" body)
  (let [result (<?? (connection/read-file-address conn resource-address))]
    {:status 200
     :body   result}))

(defhandler lookup-ledger-address
  [{:keys [fluree/conn]
    {{:keys [ledger] :as body} :body} :parameters}]
  (log/info "Retrieve ledger address request:" body)
  (let [result (<?? (connection/lookup-local-address conn ledger))]
    (log/info "got result from alias lookup:" result)
    {:status 200
     :body   {:address result}}))
