(ns fluree.server.task
  (:require [fluree.db.api :as fluree]
            [fluree.db.util.log :as log]))

(set! *warn-on-reflection* true)

(defn reindex
  [{:fluree.db/keys [connection]} ledger-name]
  (log/info "Starting reindex for ledger:" ledger-name)
  (let [result (fluree/trigger-index connection ledger-name)]
    (log/info "Reindex completed for ledger:" ledger-name)
    result))
