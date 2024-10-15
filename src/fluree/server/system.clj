(ns fluree.server.system
  (:require [clojure.string :as str]
            [fluree.db.cache :as cache]
            [fluree.db.connection :as connection]
            [fluree.db.flake.index.storage :as index-storage]
            [fluree.server.config.vocab :as vocab]
            [fluree.db.json-ld.iri :as iri]
            [fluree.db.nameservice.storage :as storage-nameservice]
            [fluree.db.remote-system :as remote-system]
            [fluree.db.serde.json :as json-serde]
            [fluree.db.storage :as storage]
            [fluree.db.storage.file :as file-storage]
            [fluree.db.storage.ipfs :as ipfs-storage]
            [fluree.db.storage.memory :as memory-storage]
            [fluree.db.storage.s3 :as s3-storage]
            [fluree.db.util.core :as util :refer [get-first get-first-value get-values]]
            [fluree.db.util.json :as json]
            [fluree.json-ld :as json-ld]
            [fluree.server.config :as config]
            [fluree.server.consensus.raft :as raft]
            [fluree.server.consensus.standalone :as standalone]
            [fluree.server.consensus.subscriptions :as subscriptions]
            [fluree.server.consensus.watcher :as watcher]
            [fluree.server.handler :as handler]
            [fluree.server.task.migrate-sid :as task.migrate-sid]
            [integrant.core :as ig]
            [ring.adapter.jetty9 :as jetty]))

(set! *warn-on-reflection* true)

(derive :fluree.server.consensus/raft :fluree.server/consensus)
(derive :fluree.server.consensus/standalone :fluree.server/consensus)

(derive :fluree.server.storage/file :fluree.server/content-storage)
(derive :fluree.server.storage/file :fluree.server/byte-storage)
(derive :fluree.server.storage/file :fluree.server/json-archive)

(derive :fluree.server.storage/memory :fluree.server/content-storage)
(derive :fluree.server.storage/memory :fluree.server/byte-storage)
(derive :fluree.server.storage/memory :fluree.server/json-archive)

(derive :fluree.server.storage/s3 :fluree.server/content-storage)
(derive :fluree.server.storage/s3 :fluree.server/byte-storage)
(derive :fluree.server.storage/s3 :fluree.server/json-archive)

(derive :fluree.server.storage/ipfs :fluree.server/content-storage)
(derive :fluree.server.storage/ipfs :fluree.server/json-archive)

(derive :fluree.server/remote-system :fluree.server/json-archive)
(derive :fluree.server/remote-system :fluree.server/nameservice)
(derive :fluree.server/remote-system :fluree.server/publication)

(derive :fluree.server.nameservice/storage :fluree.server/publisher)
(derive :fluree.server.nameservice/storage :fluree.server/nameservice)

(derive :fluree.server.nameservice/ipns :fluree.server/nameservice)
(derive :fluree.server.nameservice/ipns :fluree.server/publisher)

(derive :fluree.server.http/jetty :fluree.server/http)

(defn type?
  [node kind]
  (-> node (get-first :type) (= kind)))

(defn connection?
  [node]
  (type? node vocab/connection-type))

(defn system?
  [node]
  (type? node vocab/system-type))

(defn consensus?
  [node]
  (type? node vocab/consensus-type))

(defn raft-consensus?
  [node]
  (and (consensus? node)
       (-> node (get-first-value vocab/consensus-protocol) (= "raft"))))

(defn standalone-consensus?
  [node]
  (and (consensus? node)
       (-> node (get-first-value vocab/consensus-protocol) (= "standalone"))))

(defn http-api?
  [node]
  (and (type? node vocab/api-type)
       (contains? node vocab/http-port)))

(defn jetty-api?
  [node]
  (http-api? node))

(defn publisher?
  [node]
  (type? node vocab/publisher-type))

(defn storage-nameservice?
  [node]
  (and (publisher? node)
       (contains? node vocab/storage)))

(defn ipns-nameservice?
  [node]
  (and (publisher? node)
       (contains? node vocab/ipfs-endpoint)
       (contains? node vocab/ipns-profile)))

(defn storage?
  [node]
  (type? node vocab/storage-type))

(defn memory-storage?
  [node]
  (and (storage? node)
       (-> node
           (dissoc :idx :id :type vocab/address-identifier)
           empty?)))

(defn file-storage?
  [node]
  (and (storage? node)
       (contains? node vocab/file-path)))

(defn s3-storage?
  [node]
  (and (storage? node)
       (contains? node vocab/s3-bucket)))

(defn ipfs-storage?
  [node]
  (and (storage? node)
       (contains? node vocab/ipfs-endpoint)))

(defn get-id
  [node]
  (get node :id))

(defn derive-node-id
  [node]
  (let [id (get-id node)]
    (cond
      (connection? node)           (derive id :fluree.server/connection)
      (system? node)               (derive id :fluree.server/remote-system)
      (raft-consensus? node)       (derive id :fluree.server.consensus/raft)
      (standalone-consensus? node) (derive id :fluree.server.consensus/standalone)
      (jetty-api? node)            (derive id :fluree.server.http/jetty) ; TODO: Enable other http servers
      (memory-storage? node)       (derive id :fluree.server.storage/memory)
      (file-storage? node)         (derive id :fluree.server.storage/file)
      (s3-storage? node)           (derive id :fluree.server.storage/s3)
      (ipfs-storage? node)         (derive id :fluree.server.storage/ipfs)
      (ipns-nameservice? node)     (derive id :fluree.server.nameservice/ipns)
      (storage-nameservice? node)  (derive id :fluree.server.nameservice/storage))
    node))

(defn subject-node?
  [x]
  (and (map? x)
       (not (contains? x :value))))

(defn blank-node?
  [x]
  (and (subject-node? x)
       (not (contains? x :id))))

(defn ref-node?
  [x]
  (and (subject-node? x)
       (not (blank-node? x))
       (-> x
           (dissoc :idx)
           count
           (= 1))))

(defn split-subject-node
  [node]
  (let [node* (cond-> node
                (blank-node? node) (assoc :id (iri/new-blank-node-id))
                true               (dissoc :idx))]
    (if (ref-node? node*)
      [node*]
      (let [ref-node (select-keys node* [:id])]
        [ref-node node*]))))

(defn flatten-sequence
  [coll]
  (loop [[child & r]   coll
         child-nodes   []
         flat-sequence []]
    (if child
      (if (subject-node? child)
        (let [[ref-node child-node] (split-subject-node child)
              child-nodes*          (if child-node
                                      (conj child-nodes child-node)
                                      child-nodes)]
          (recur r child-nodes* (conj flat-sequence ref-node)))
        (recur r child-nodes (conj flat-sequence child)))
      [flat-sequence child-nodes])))

(defn flatten-node
  [node]
  (loop [[[k v] & r] (dissoc node :idx)
         children    []
         flat-node   {}]
    (if k
      (if (sequential? v)
        (let [[flat-sequence child-nodes] (flatten-sequence v)]
          (recur r
                 (into children child-nodes)
                 (assoc flat-node k flat-sequence)))
        (if (and (subject-node? v)
                 (not (ref-node? v)))
          (let [[ref-node child-node] (split-subject-node v)]
            (recur r (conj children child-node) (assoc flat-node k ref-node)))
          (recur r children (assoc flat-node k v))))
      [flat-node children])))

(defn flatten-nodes
  [nodes]
  (loop [remaining nodes
         flattened []]
    (if-let [node (peek remaining)]
      (let [[flat-node children] (flatten-node node)
            remaining*           (-> remaining
                                     pop
                                     (into children))
            flattened*           (conj flattened flat-node)]
        (recur remaining* flattened*))
      flattened)))

(defn encode-illegal-char
  [c]
  (case c
    "&" "<am>"
    "@" "<at>"
    "]" "<cb>"
    ")" "<cp>"
    ":" "<cl>"
    "," "<cm>"
    "$" "<dl>"
    "." "<do>"
    "%" "<pe>"
    "#" "<po>"
    "(" "<op>"
    "[" "<ob>"
    ";" "<sc>"
    "/" "<sl>"))

(defn kw-encode
  [s]
  (str/replace s #"[:#@$&%.,;~/\(\)\[\]]" encode-illegal-char))

(defn iri->kw
  [iri]
  (let [iri* (or iri (iri/new-blank-node-id))]
    (->> (iri/decompose iri*)
         (map kw-encode)
         (apply keyword))))

(defn keywordize-node-id
  [node]
  (if (subject-node? node)
    (update node :id iri->kw)
    node))

(defn keywordize-child-ids
  [node]
  (into {}
        (map (fn [[k v]]
               (let [v* (if (coll? v)
                          (map keywordize-node-id v)
                          (keywordize-node-id v))]
                 [k v*])))
        node))

(defn keywordize-node-ids
  [node]
  (-> node keywordize-node-id keywordize-child-ids))

(defn reference?
  [node]
  (and (map? node)
       (contains? node :id)
       (-> node (dissoc :idx :id) empty?)))

(defn convert-reference
  [node]
  (if (reference? node)
    (let [id (get-id node)]
      (ig/ref id))
    node))

(defn convert-references
  [node]
  (into {}
        (map (fn [[k v]]
               (let [v* (if (coll? v)
                          (mapv convert-reference v)
                          (convert-reference v))]
                 [k v*])))
        node))

(defmethod ig/expand-key :fluree.server/connection
  [k config]
  (let [cache-max-mb   (get-first-value config vocab/cache-max-mb)
        commit-storage (get-first config vocab/commit-storage)
        index-storage  (get-first config vocab/index-storage)
        remote-systems (get config vocab/remote-systems)
        serializer     (json-serde/json-serde)
        config*        (-> config
                           (assoc :cache (ig/ref :fluree.server/cache)
                                  :commit-catalog (ig/ref :fluree.server/commit-catalog)
                                  :index-catalog (ig/ref :fluree.server/index-catalog)
                                  :serializer serializer)
                           (dissoc vocab/cache-max-mb vocab/commit-storage
                                   vocab/index-storage))]
    {:fluree.server/cache          cache-max-mb
     :fluree.server/commit-catalog {:content-stores     [commit-storage]
                                    :read-only-archives remote-systems}
     :fluree.server/index-catalog  {:content-stores     [index-storage]
                                    :read-only-archives remote-systems
                                    :cache              (ig/ref :fluree.server/cache)
                                    :serializer         serializer}
     k                             config*}))

(defmethod ig/expand-key :fluree.server/http
  [k config]
  (let [max-txn-wait-ms (get-first-value config vocab/max-txn-wait-ms)
        closed-mode     (get-first-value config vocab/closed-mode)
        root-identities (set (get-values config vocab/root-identities))
        config*         (-> config
                            (assoc :handler (ig/ref :fluree.server.api/handler))
                            (dissoc vocab/max-txn-wait-ms vocab/closed-mode
                                    vocab/root-identities))]
    {k                          config*
     :fluree.server/watcher     {:max-txn-wait-ms max-txn-wait-ms}
     :fluree.server.api/handler {:root-identities root-identities
                                 :closed-mode     closed-mode
                                 :connection      (ig/ref :fluree.server/connection)
                                 :consensus       (ig/ref :fluree.server/consensus)
                                 :watcher         (ig/ref :fluree.server/watcher)
                                 :subscriptions   (ig/ref :fluree.server/subscriptions)}}))

(defmethod ig/expand-key :fluree.server.consensus/standalone
  [k config]
  {k (assoc config
            :subscriptions (ig/ref :fluree.server/subscriptions)
            :watcher (ig/ref :fluree.server/watcher))})

(defmethod ig/init-key :fluree.server.storage/memory
  [_ config]
  (let [identifier (get-first-value config vocab/address-identifier)]
    (memory-storage/open identifier)))

(defmethod ig/init-key :fluree.server.storage/file
  [_ config]
  (let [identifier (get-first-value config vocab/address-identifier)
        root-path  (get-first-value config vocab/file-path)]
    (file-storage/open identifier root-path)))

(defmethod ig/init-key :fluree.server.storage/s3
  [_ config]
  (let [identifier  (get-first-value config vocab/address-identifier)
        s3-bucket   (get-first-value config vocab/s3-bucket)
        s3-prefix   (get-first-value config vocab/s3-prefix)
        s3-endpoint (get-first-value config vocab/s3-endpoint)]
    (s3-storage/open identifier s3-bucket s3-prefix s3-endpoint)))

(defmethod ig/init-key :fluree.server.storage/ipfs
  [_ config]
  (let [identifier    (get-first-value config vocab/address-identifier)
        ipfs-endpoint (get-first-value config vocab/ipfs-endpoint)]
    (ipfs-storage/open identifier ipfs-endpoint)))

(defmethod ig/init-key :fluree.server.nameservice/storage
  [_ config]
  (let [storage (get-first config vocab/storage)]
    (storage-nameservice/start storage)))

(defmethod ig/init-key :fluree.server/remote-system
  [_ config]
  (let [urls        (get-values config vocab/server-urls)
        identifiers (get-values config vocab/address-identifiers)]
    (remote-system/connect urls identifiers)))

(defmethod ig/init-key :fluree.server/commit-catalog
  [_ {:keys [content-stores read-only-archives]}]
  (storage/catalog content-stores read-only-archives))

(defmethod ig/init-key :fluree.server/index-catalog
  [_ {:keys [content-stores read-only-archives serializer cache]}]
  (let [catalog (storage/catalog content-stores read-only-archives)]
    (index-storage/index-catalog catalog serializer cache)))

(defmethod ig/init-key :fluree.server/cache
  [_ max-mb]
  (-> max-mb cache/memory->cache-size cache/create-lru-cache atom))

(defmethod ig/init-key :fluree.server/connection
  [_ {:keys [cache commit-catalog index-catalog serializer] :as config}]
  (let [parallelism          (get-first-value config vocab/parallelism)
        primary-publisher    (get-first config vocab/primary-publisher)
        secondary-publishers (get config vocab/secondary-publishers)
        remote-systems       (get config vocab/remote-systems)
        ledger-defaults      (get-first config vocab/ledger-defaults)
        index-options        (get-first ledger-defaults vocab/index-options)
        reindex-min-bytes    (get-first index-options vocab/reindex-min-bytes)
        reindex-max-bytes    (get-first index-options vocab/reindex-max-bytes)
        max-old-indexes      (get-first index-options vocab/max-old-indexes)
        ledger-defaults*     {:index-options {:reindex-min-bytes reindex-min-bytes
                                              :reindex-max-bytes reindex-max-bytes
                                              :max-old-indexes   max-old-indexes}}]
    (connection/connect {:parallelism          parallelism
                         :cache                cache
                         :commit-catalog       commit-catalog
                         :index-catalog        index-catalog
                         :primary-publisher    primary-publisher
                         :secondary-publishers secondary-publishers
                         :remote-systems       remote-systems
                         :serializer           serializer
                         :defaults             ledger-defaults*})))

(defmethod ig/init-key :fluree.server/subscriptions
  [_ _]
  (subscriptions/listen))

(defmethod ig/halt-key! :fluree.server/subscriptions
  [_ subsc]
  (subscriptions/close subsc))

(defmethod ig/init-key :fluree.server/watcher
  [_ {:keys [max-txn-wait-ms]}]
  (watcher/watch max-txn-wait-ms))

(defmethod ig/halt-key! :fluree.server/watcher
  [_ watcher]
  (watcher/close watcher))

(defmethod ig/init-key :fluree.server.consensus/raft
  [_ config]
  (let [log-history      (get-first-value config vocab/log-history)
        entries-max      (get-first-value config vocab/entries-max)
        catch-up-rounds  (get-first-value config vocab/catch-up-rounds)
        servers          (get-values config vocab/raft-servers)
        this-server      (get-first-value config vocab/this-server)
        log-directory    (get-first-value config vocab/log-directory)
        ledger-directory (get-first-value config vocab/ledger-directory)]
    (raft/start {:log-history      log-history
                 :entries-max      entries-max
                 :catch-up-rounds  catch-up-rounds
                 :servers          servers
                 :this-server      this-server
                 :log-directory    log-directory
                 :ledger-directory ledger-directory})))

(defmethod ig/halt-key! :fluree.server.consensus/raft
  [_ {:keys [close] :as _raft-group}]
  (close))

(defmethod ig/init-key :fluree.server.consensus/standalone
  [_ {:keys [subscriptions watcher] :as config}]
  (let [max-pending-txns (get-first-value config vocab/max-pending-txns)
        conn             (get-first config vocab/connection)]
    (standalone/start conn subscriptions watcher max-pending-txns)))

(defmethod ig/halt-key! :fluree.server.consensus/standalone
  [_ transactor]
  (standalone/stop transactor))

(defmethod ig/init-key :fluree.server.api/handler
  [_ {:keys [connection consensus watcher subscriptions root-identities closed-mode]}]
  (handler/app connection consensus watcher subscriptions root-identities closed-mode))

(defmethod ig/init-key :fluree.server.http/jetty
  [_ {:keys [handler] :as config}]
  (let [port (get-first-value config vocab/http-port)]
    (jetty/run-jetty handler {:port port, :join? false})))

(defmethod ig/halt-key! :fluree.server.http/jetty
  [_ http-server]
  (jetty/stop-server http-server))

(defmethod ig/init-key :fluree.server/sid-migration
  [_ {:keys [conn ledgers force]}]
  (task.migrate-sid/migrate conn ledgers force))

(defmethod ig/init-key :default
  [_ config]
  config)

(def default-resource-name "file-config.jsonld")

(def base-config
  {:fluree.server/subscriptions {}})

(defn parse
  [config]
  (->> config
       flatten-nodes
       (map keywordize-node-ids)
       (map derive-node-id)
       (map convert-references)
       (map (juxt get-id identity))
       (into base-config)))

(defn start-config
  ([config]
   (start-config config nil))
  ([config _profile]
   (-> config json-ld/expand util/sequential parse ig/expand ig/init)))

(defn start-file
  ([path]
   (start-file path :prod))
  ([path profile]
   (-> path
       config/read-file
       (start-config profile))))

(defn start-resource
  ([resource-name]
   (start-resource resource-name :prod))
  ([resource-name profile]
   (-> resource-name
       config/read-resource
       (start-config profile))))

(def start
  (partial start-resource default-resource-name))

(defn stop
  [server]
  (ig/halt! server))
