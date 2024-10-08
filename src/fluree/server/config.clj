(ns fluree.server.config
  (:refer-clojure :exclude [load-file])
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
    ::auth-id string?
    ::connection-storage-method [:enum
                                 :ipfs :file :memory :s3 :remote]
    ::indexing-options [:map
                        [:reindex-min-bytes {:optional true} pos-int?]
                        [:reindex-max-bytes {:optional true} pos-int?]
                        [:max-old-indexes {:optional true} nat-int?]]
    ::connection-defaults [:map
                           [:index {:optional true} ::indexing-options]
                           [:did {:optional true} :string]]
    ::file-connection [:map]
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
                   [:parallelism {:optional true} pos-int?]
                   [:defaults {:optional true} ::connection-defaults]]
                  [:multi {:dispatch :storage-method}
                   [:file ::file-connection]
                   [:memory ::memory-connection]
                   [:ipfs ::ipfs-connection]
                   [:remote ::remote-connection]
                   [:s3 ::s3-connection]]]
    ::server-config [:map
                     [:cache-max-mb {:optional true} pos-int?]
                     [:storage-path {:optional true} ::path]]
    ::consensus-protocol [:enum
                          :raft :standalone]
    ::raft [:map
            [:log-history {:optional true} pos-int?]
            [:entries-max {:optional true} pos-int?]
            [:catch-up-rounds {:optional true} pos-int?]
            [:servers [:sequential ::server-address]]
            [:this-server ::server-address]
            [:log-directory {:optional true} ::path]]
    ::standalone [:map [:max-pending-txns {:optional true} pos-int?]]
    ::consensus [:and
                 [:map [:protocol ::consensus-protocol]]
                 [:multi {:dispatch :protocol}
                  [:raft ::raft]
                  [:standalone ::standalone]]]
    ::http-server [:enum
                   :jetty]
    ::http-port pos-int?
    ::max-txn-wait-ms pos-int?
    ::jetty [:map [:server ::http-server]]
    ::http [:and
            [:map
             [:server ::http-server]
             [:port ::http-port]
             [:max-txn-wait-ms {:optional true} ::max-txn-wait-ms]
             [:root-identities {:optional true} [:sequential ::auth-id]]
             [:closed-mode {:optional true} :boolean]]
            [:multi {:dispatch :server}
             [:jetty ::jetty]]]
    ::config [:map {:closed true}
              [:server ::server-config]
              [:connection ::connection]
              [:consensus ::consensus]
              [:http ::http]]

    ::sid-migration [:map
                     [:ledgers {:doc "Collection of ledger aliases to migrate."} [:sequential :string]]
                     [:force {:optional true :doc "If true, run the migration regardless of whether the ledger has already been migrated."}
                      :boolean]]

    ::sid-migration-config [:map {:closed true}
                            [:server ::server-config]
                            [:connection ::connection]
                            [:sid-migration ::sid-migration]]}))

(def coerce
  (m/coercer [:or ::config ::sid-migration-config] transform/string-transformer {:registry registry}))

(def env-template
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

(defn apply-overrides
  [config profile]
  (let [profile-overrides (get-in config [:profiles profile])
        env-overrides     (env-config)]
    (-> config
        (dissoc :profiles)
        (deep-merge profile-overrides env-overrides))))

(defn with-config-ns
  [k]
  (keyword "fluree.server.config" (name k)))

(defn with-namespaced-keys
  [cfg]
  (reduce-kv (fn [m k v]
               (assoc m (with-config-ns k) v))
             {} cfg))

(defn finalize
  [config profile]
  (-> config
      (apply-overrides profile)
      coerce
      with-namespaced-keys))

(defn parse-config
  [cfg]
  (json/parse cfg ->kebab-case-keyword))

(defn read-resource
  [resource-name]
  (-> resource-name
      io/resource
      slurp
      parse-config))

(defn load-resource
  ([resource-name]
   (load-resource resource-name nil))

  ([resource-name profile]
   (-> resource-name
       read-resource
       (finalize profile))))

(defn read-file
  [path]
  (-> path
      io/file
      slurp
      parse-config))

(defn load-file
  ([path]
   (load-file path nil))

  ([path profile]
   (-> path
       read-file
       (finalize profile))))
