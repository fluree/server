(ns fluree.server.handlers.ledger
  (:require
   [fluree.db.api :as fluree]
   [fluree.db.util.log :as log]
   [fluree.server.handler :as-alias handler]
   [fluree.server.handlers.shared :refer [defhandler deref!]]))

(defhandler query
  [{:keys [fluree/conn fluree/opts] {:keys [body]} :parameters :as _req}]
  (let [query (or (::handler/query body) body)]
    (log/debug "query handler received query:" query opts)
    {:status 200
     :body   (deref! (fluree/query-connection conn query opts))}))

(defhandler history
  [{:keys [fluree/conn fluree/opts] {{ledger :from :as query} :body} :parameters :as _req}]

  (let [ledger*       (->> ledger (fluree/load conn) deref!)
        query*        (dissoc query :from)]
    (log/debug "history handler received query:" query opts)
    {:status 200
     :body   (deref! (fluree/history ledger* query* opts))}))
