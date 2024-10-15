(ns fluree.server.system
  (:require [fluree.db.cache :as cache]
            [fluree.db.connection :as connection]
            [fluree.db.flake.index.storage :as index-storage]
            [fluree.server.config.vocab :as vocab]
            [fluree.db.nameservice.storage :as storage-nameservice]
            [fluree.db.remote-system :as remote-system]
            [fluree.db.serde.json :as json-serde]
            [fluree.db.storage :as storage]
            [fluree.db.storage.file :as file-storage]
            [fluree.db.storage.ipfs :as ipfs-storage]
            [fluree.db.storage.memory :as memory-storage]
            [fluree.db.storage.s3 :as s3-storage]
            [fluree.db.util.core :as util :refer [get-id get-first get-first-value get-values]]
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

(defn convert-node-references
  [node]
  (reduce-kv (fn [m k v]
               (let [v* (if (coll? v)
                          (mapv convert-reference v)
                          (convert-reference v))]
                 (assoc m k v*)))
             {} node))

(defn convert-references
  [cfg]
  (reduce-kv (fn [m id node]
               (assoc m id (convert-node-references node)))
             {} cfg))

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

(defn start-config
  ([config]
   (start-config config nil))
  ([config _profile]
   (-> config config/parse convert-references ig/expand ig/init)))

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
