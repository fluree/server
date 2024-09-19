(ns fluree.server.handlers.ledger
  (:require
   [fluree.db.api :as fluree]
   [fluree.db.util.log :as log]
   [fluree.server.handler :as-alias handler]
   [fluree.server.handlers.shared :refer [defhandler deref!]]))

(defhandler query
  [{:keys [fluree/conn credential/did root/trusted-user]
    {:keys [body]} :parameters}]
  (let [query  (or (::handler/query body) body)
        format (or (::handler/format body) :fql)
        _      (log/debug "query handler received query:" query)
        opts   (when (= :fql format)
                 (cond-> (:opts query)
                   (and did (not trusted-user)) (assoc :did did)))
        query* (if opts (assoc query :opts opts) query)]
    {:status 200
     :body   (deref! (fluree/query-connection conn query* {:format format}))}))

(defhandler history
  [{:keys [fluree/conn credential/did root/trusted-user]
    {{ledger :from :as query} :body} :parameters}]
  (log/debug "history handler got query:" query)
  (let [ledger* (->> ledger (fluree/load conn) deref!)
        opts    (cond-> (or (:opts query) {})
                  (and did (not trusted-user)) (assoc :identity did))
        query*  (dissoc query :from)
        _       (log/debug "history - Querying ledger" ledger "-" query*)
        results (deref! (fluree/history ledger* query* opts))]
    (log/debug "history - query results:" results)
    {:status 200
     :body   results}))
