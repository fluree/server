(ns fluree.server.config
  (:require [camel-snake-kebab.core :refer [->kebab-case-keyword]]
            [clojure.java.io :as io]
            [clojure.walk :as walk]
            [fluree.db.util.json :as json]
            [malli.core :as m]
            [malli.transform :as transform]))

(def registry
  (merge
    (m/predicate-schemas)
    (m/class-schemas)
    (m/comparator-schemas)
    (m/type-schemas)
    (m/sequence-schemas)
    (m/base-schemas)
    {::path string?
     ::server-address string?
     ::connection-storage-method [:enum {:decode/config keyword}
                                  :ipfs :file :memory :s3 :remote]
     ::indexing-options [:map
                         [:reindex-min-bytes pos-int?]
                         [:reindex-max-bytes pos-int?]]
     ::connection-defaults [:map
                            [:index ::indexing-options]
                            [:did :string]]
     ::file-connection [:map [:storage-path ::path]]
     ::memory-connection [:map]
     ::ipfs-connection [:map [:ipfs-server ::server-address]]
     ::remote-connection [:map [:remote-servers [:sequential ::server-address]]]
     ::s3-connection [:map
                      [:s3-endpoint :string]
                      [:s3-bucket :string]
                      [:s3-prefix :string]]
     ::connection [:and
                   [:map
                    [:storage-method ::connection-storage-method]
                    [:parallelism pos-int?]
                    [:cache-max-mb pos-int?]
                    [:defaults ::connection-defaults]]
                   [:multi {:dispatch :storage-method}
                    [:file ::file-connection]
                    [:memory ::memory-connection]
                    [:ipfs ::ipfs-connection]
                    [:remote ::remote-connection]
                    [:s3 ::s3-connection]]]
     ::consensus-protocol [:enum {:decode/config keyword}
                           :raft :standalone]
     ::raft [:map
             [:log-history pos-int?]
             [:entries-max pos-int?]
             [:catch-up-rounds pos-int?]
             [:servers [:sequential ::server-address]]
             [:this-server ::server-address]
             [:log-directory ::path]
             [:ledger-directory ::path]]
     ::standalone [:map [:max-pending-txns pos-int?]]
     ::consensus [:and
                  [:map [:protocol ::consensus-protocol]]
                  [:multi {:dispatch :protocol}
                   [:raft ::raft]
                   [:standalone ::standalone]]]
     ::http-server [:enum {:decode/config keyword}
                    :jetty]
     ::http-port pos-int?
     ::max-txn-wait-ms pos-int?
     ::jetty [:map [:server ::http-server]]
     ::http [:and
             [:map
              [:server ::http-server]
              [:port ::http-port]
              [:max-txn-wait-ms ::max-txn-wait-ms]]
             [:multi {:dispatch :server}
              [:jetty ::jetty]]]
     ::config [:map {:closed true}
               [:connection ::connection]
               [:consensus ::consensus]
               [:http ::http]]}))

(def env-template
  {:connection {:remote-servers "FLUREE_REMOTE_SERVERS"
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
                :ledger-directory "FLUREE_RAFT_LEDGER_DIRECTORY"}
   :http       {:server          "FLUREE_HTTP_SERVER"
                :port            "FLUREE_HTTP_API_PORT"
                :max-txn-wait-ms "FLUREE_HTTP_MAX_TXN_WAIT_MS"}})

(defn env-config
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
