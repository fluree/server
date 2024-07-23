(ns fluree.server.config
  (:require [camel-snake-kebab.core :refer [->kebab-case-keyword]]
            [clojure.java.io :as io]
            [clojure.walk :as walk]
            [fluree.db.util.json :as json]
            [malli.core :as m]))


(def env-template
  {:fluree {:connection {:remote-servers "FLUREE_REMOTE_SERVERS"
                         :storage-method "FLUREE_STORAGE_METHOD"
                         :parallelism    "FLUREE_CONNECTION_PARALLELISM"
                         :storage-path   "FLUREE_STORAGE_PATH"
                         :cache-max-mb   "FLUREE_CACHE_MAX_MB"
                         :defaults       {:index {:reindex-max-bytes "FLUREE_REINDEX_MAX_BYTES"
                                                  :reindex-min-bytes "FLUREE_REINDEX_MIN_BYTES"}}}
            :consensus  {:protocol         "FLUREE_CONSENSUS_PROTOCOL"
                         :max-pending-txns "FLUREE_STANDALONE_MAX_PENDING_TXNS"
                         :log-history      "FLUREE_RAFT_LOG_HISTORY"
                         :entries-max      "FLUREE_RAFT_ENTRIES_MAX"
                         :catch-up-rounds  "FLUREE_RAFT_CATCH_UP_ROUNDS"
                         :storage-type     "FLUREE_RAFT_STORAGE_TYPE"
                         :servers          "FLUREE_RAFT_SERVERS"
                         :this-server      "FLUREE_RAFT_THIS_SERVER"
                         :log-directory    "FLUREE_RAFT_LOG_DIRECTORY"
                         :ledger-directory "FLUREE_RAFT_LEDGER_DIRECTORY"}}
   :http   {:server "FLUREE_HTTP_SERVER"
            :port   "FLUREE_HTTP_API_PORT"}})

(defn env-config
  []
  (walk/postwalk (fn [x]
                   (cond (string? x)    (System/getenv x)
                         (map-entry? x) (when (some? (val x))
                                          x)
                         (coll? x)      (->> x
                                             (into (empty x) (remove nil?))
                                             not-empty)
                         :else          x))
                 env-template))

(defn deep-merge
  ([x y]
   (if (and (map? x) (map? y))
     (merge-with deep-merge x y)
     (if (some? y)
       y
       x)))
  ([x y & more]
   (reduce deep-merge x (cons y more))))

(defn read-resource
  [resource-name]
  (-> resource-name
      io/resource
      slurp
      (json/parse ->kebab-case-keyword)))

(defn load-resource
  ([resource-name]
   (load-resource resource-name nil))

  ([resource-name profile]
   (let [config            (read-resource resource-name)
         profile-overrides (get-in config [:profiles profile])
         env-overrides     (env-config)]
     (-> config
         (dissoc :profiles)
         (deep-merge profile-overrides env-overrides)))))
