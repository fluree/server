(ns fluree.server.handlers.ledger
  (:require [fluree.db.api :as fluree]
            [fluree.db.util.log :as log]
            [fluree.server.handler :as-alias handler]
            [fluree.server.handlers.shared :refer [defhandler deref!] :as shared]))

(defhandler query
  [{:keys [fluree/conn fluree/opts] {:keys [body]} :parameters :as _req}]
  (let [query (or (::handler/query body) body)
        {:keys [status result] :as query-response}
        (deref! (fluree/query-connection conn query opts))]
    (log/debug "query handler received query:" query opts)
    (shared/with-tracking-headers {:status status, :body result}
      query-response)))

(defhandler history
  [{:keys [fluree/conn fluree/opts] {{ledger :from :as query} :body} :parameters :as _req}]

  (let [query*  (dissoc query :from)

        {:keys [status result] :as history-response}
        (deref! (fluree/history conn ledger query* opts))]
    (log/debug "history handler received query:" query opts)
    (shared/with-tracking-headers {:status status, :body result}
      history-response)))
