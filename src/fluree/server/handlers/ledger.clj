(ns fluree.server.handlers.ledger
  (:require [fluree.db.api :as fluree]
            [fluree.db.util.log :as log]
            [fluree.db.util.trace :as trace]
            [fluree.server.handler :as-alias handler]
            [fluree.server.handlers.shared :refer [defhandler deref!] :as shared]))

(defhandler query
  [{:keys [fluree/conn fluree/opts] {:keys [body path]} :parameters :as _req}]
  (let [query (or (::handler/query body) body)
        ;; supply ledger-alias from path params if not overridden by a header
        opts* (update opts :ledger #(or % (:ledger-alias path)))
        {:keys [status result] :as query-response}
        (trace/form ::query-handler {}
          (deref! (fluree/query-connection conn query opts*)))]
    (log/debug "query handler received query:" query opts*)
    (shared/with-tracking-headers {:status status, :body result}
      query-response)))

(defhandler history
  [{:keys [fluree/conn fluree/opts] {{ledger :from :as query} :body} :parameters :as _req}]
  (let [query*  (dissoc query :from)
        result  (trace/form ::history-handler {}
                  (deref! (fluree/history conn ledger query* opts)))]
    (log/debug "history handler received query:" query opts "result:" result)
    ;; fluree/history may return either raw result or wrapped in {:status :result}
    (if (and (map? result) (:status result) (:result result))
      {:status (:status result), :body (:result result)}
      {:status 200, :body result})))
