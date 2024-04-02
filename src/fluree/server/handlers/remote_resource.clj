(ns fluree.server.handlers.remote-resource
  (:require [fluree.db.json-ld.api :as-alias fluree]
            [fluree.db.nameservice.core :as nameservice]
            [fluree.db.util.async :refer [<??]]
            [fluree.db.util.log :as log]
            [fluree.server.handlers.shared :refer [defhandler]]))

(defhandler read-handler
  [{:keys [fluree/conn]
    {{resource-address :resource :as body} :body} :parameters}]
  (log/debug "Remote resource read request:" body)
  (let [result (<?? (nameservice/read-resource conn resource-address))]
    {:status 200
     :body   result}))
