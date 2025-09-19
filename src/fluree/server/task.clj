(ns fluree.server.task
  (:require [clojure.core.async :as async]
            [fluree.db.connection :as connection]
            [fluree.db.constants :as const]
            [fluree.db.nameservice :as nameservice]
            [fluree.db.util :as util]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.log :as log]))

(set! *warn-on-reflection* true)

(defn extract-ledger-aliases
  [ns-records]
  (mapv #(get % const/iri-id) ns-records))

(defn reindex-single
  [conn alias]
  (async/go
    (log/info "Starting reindex for ledger:" alias)
    (let [result (async/<! (connection/trigger-ledger-index conn alias nil))]
      (if (util/exception? result)
        (do
          (log/error result "Reindex failed for ledger:" alias)
          alias)
        (do
          (log/info "Reindex completed for ledger:" alias)
          :reindexed)))))

(defn reindex-all
  [{:keys [parallelism primary-publisher] :as conn
    :or {parallelism 4}}]
  (go-try
    (log/info "Starting reindex for all ledgers")
    (let [records    (<? (nameservice/all-records primary-publisher))
          aliases    (extract-ledger-aliases records)
          in-ch      (async/to-chan! aliases)
          out-ch     (async/chan parallelism (remove #{:reindexed}))
          reindex-fn (fn [alias ch]
                       (-> (reindex-single conn alias)
                           (async/pipe ch)))]
      (log/info "Found" (count aliases) "ledgers to reindex:" aliases)
      (async/pipeline-async parallelism out-ch reindex-fn in-ch)
      (let [failed (<? (async/into [] out-ch))]
        (if (not-empty failed)
          (do (log/warn "Reindexing failed for ledgers:" failed)
              :reindexing-failures)
          (do (log/info "Reindex completed for all ledgers")
              :reindexed-all))))))

(defn reindex
  "Returns a vector of dbs for "
  [conn alias]
  (if (= alias "--all")
    (reindex-all conn)
    (reindex-single conn alias)))
