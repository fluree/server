(ns fluree.server.handlers.ledger
  (:require
   [fluree.db.json-ld.api :as fluree]
   [fluree.db.util.log :as log]
   [fluree.server.components.http :as-alias http]
   [fluree.server.handlers.shared :refer [defhandler deref!]]))

(defhandler query
  [{:keys [fluree/conn credential/did credential/role]
    {:keys [body]} :parameters}]
  (let [query        (or (::http/query body) body)
        format       (or (::http/format body) :fql)
        _            (log/debug "query handler received query:" query)
        request-opts {:format format :did did :role role}]
    {:status 200
     :body   (deref! (fluree/query-connection conn query request-opts))}))

(defhandler history
  [{:keys [fluree/conn credential/did]
    {{ledger :from :as query} :body} :parameters}]
  (log/debug "history handler got query:" query)
  (let [ledger* (->> ledger (fluree/load conn) deref!)
        opts    (cond-> (or (:opts query) {})
                  did (assoc :did did))
        query*  (-> query
                    (dissoc :from)
                    (assoc :opts opts))
        _       (log/debug "history - Querying ledger" ledger "-" query*)
        results (deref! (fluree/history ledger* query*))]
    (log/debug "history - query results:" results)
    {:status 200
     :body   results}))
