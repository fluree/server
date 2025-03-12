(ns fluree.server.handlers.ledger
  (:require
   [fluree.db.api :as fluree]
   [fluree.db.util.log :as log]
   [fluree.server.handler :as-alias handler]
   [fluree.server.handlers.shared :refer [defhandler deref!]]
   [steffan-westcott.clj-otel.api.trace.span :as span]))

(defn add-policy-enforcement-headers
  [override-opts {:keys [credential/did policy/identity policy/class policy/policy policy/values]}]
  (cond-> override-opts
    identity (assoc :identity identity)
    did (assoc :identity did) ;; a credential will always override the policy/identity header
    class (assoc :policy-class class)
    policy (assoc :policy policy)
    values (assoc :policy-values values)))

(defhandler query
  [{:keys [fluree/conn] {:keys [body]} :parameters :as req}]
  (let [query         (or (::handler/query body) body)
        format        (or (::handler/format body) :fql)
        _             (log/debug "query handler received query:" query)
        override-opts (add-policy-enforcement-headers {:format format} req)]
    ;; todo; add any useful request data to mdc and span
    {:status 200
     :body   (deref! (fluree/query-connection conn query override-opts))}))

(defhandler history
  [{:keys [fluree/conn] {{ledger :from :as query} :body} :parameters :as req}]
  (log/with-mdc {:ledger.id ledger}
    (span/add-span-data! {:attributes (org.slf4j.MDC/getCopyOfContextMap)})
    (log/debug "history handler got query:" query)
    (let [ledger*       (->> ledger (fluree/load conn) deref!)
          override-opts (add-policy-enforcement-headers {} req)
          query*        (dissoc query :from)
          _             (log/debug "history - Querying ledger" ledger "-" query*)
          results       (deref! (fluree/history ledger* query* override-opts))]

      (log/debug "history - query results:" results)
      {:status 200
       :body   results})))
