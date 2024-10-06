(ns fluree.server.config.env
  (:require [clojure.walk :as walk]))

(def template
  {:server     {:storage-path "FLUREE_STORAGE_PATH"}
   :connection {:storage-method "FLUREE_STORAGE_METHOD"
                :parallelism    "FLUREE_CONNECTION_PARALLELISM"
                :cache-max-mb   "FLUREE_CACHE_MAX_MB"
                :remote-servers "FLUREE_REMOTE_SERVERS"
                :ipfs-server    "FLUREE_IPFS_SERVER"
                :s3-endpoint    "FLUREE_S3_ENDPOINT"
                :s3-bucket      "FLUREE_S3_BUCKET"
                :s3-prefix      "FLUREE_S3_PREFIX"
                :defaults       {:index {:reindex-max-bytes "FLUREE_REINDEX_MAX_BYTES"
                                         :reindex-min-bytes "FLUREE_REINDEX_MIN_BYTES"
                                         :max-old-indexes   "FLUREE_MAX_OLD_INDEXES"}
                                 :did   "FLUREE_DEFAULT_DID"}}
   :consensus  {:protocol         "FLUREE_CONSENSUS_PROTOCOL"
                :max-pending-txns "FLUREE_STANDALONE_MAX_PENDING_TXNS"
                :log-history      "FLUREE_RAFT_LOG_HISTORY"
                :entries-max      "FLUREE_RAFT_ENTRIES_MAX"
                :catch-up-rounds  "FLUREE_RAFT_CATCH_UP_ROUNDS"
                :storage-type     "FLUREE_RAFT_STORAGE_TYPE"
                :servers          "FLUREE_RAFT_SERVERS"
                :this-server      "FLUREE_RAFT_THIS_SERVER"
                :log-directory    "FLUREE_RAFT_LOG_DIRECTORY"}
   :http       {:server          "FLUREE_HTTP_SERVER"
                :port            "FLUREE_HTTP_API_PORT"
                :max-txn-wait-ms "FLUREE_HTTP_MAX_TXN_WAIT_MS"}})

(defn config
  []
  (walk/postwalk (fn [x]
                   (cond (string? x)    (System/getenv x)
                         (map-entry? x) (when (some? (val x))
                                          x)
                         (coll? x)      (->> x
                                             (remove nil?)
                                             (into (empty x))
                                             not-empty)
                         :else          x))
                 template))
