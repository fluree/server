(ns fluree.server.system
  (:require [clojure.string :as str]
            [fluree.db.connection :as connection]
            [fluree.db.json-ld.iri :as iri]
            [fluree.db.nameservice.storage :as storage-nameservice]
            [fluree.db.remote-system :as remote-system]
            [fluree.db.storage.file :as file-storage]
            [fluree.db.storage.ipfs :as ipfs-storage]
            [fluree.db.storage.memory :as memory-storage]
            [fluree.db.storage.s3 :as s3-storage]
            [fluree.db.util.core :as util :refer [get-first get-first-value]]
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

(def system-ns
  "https://ns.flur.ee/system#")

(defn system-iri
  [s]
  (str system-ns s))

(def connection-type
  (system-iri "Connection"))

(def consensus-type
  (system-iri "Consensus"))

(def storage-type
  (system-iri "Storage"))

(def publisher-type
  (system-iri "Publisher"))

(def system-type
  (system-iri "System"))

(def api-type
  (system-iri "API"))

(def address-identifier-iri
  (system-iri "addressIdentifier"))

(def address-identifiers-iri
  (system-iri "addressIdentifiers"))

(def file-path-iri
  (system-iri "filePath"))

(def s3-bucket-iri
  (system-iri "s3Bucket"))

(def s3-prefix-iri
  (system-iri "s3Prefix"))

(def s3-endpoint-iri
  (system-iri "s3Endpoint"))

(def storage-iri
  (system-iri "storage"))

(def ipfs-endpoint-iri
  (system-iri "ipfsEndpoint"))

(def ipns-profile-iri
  (system-iri "ipnsProfile"))

(def consensus-protocol-iri
  (system-iri "consensusProtocol"))

(def http-port-iri
  (system-iri "httpPort"))

(def max-txn-wait-ms-iri
  (system-iri "maxTxnWaitMs"))

(def parallelism-iri
  (system-iri "parallelism"))

(def cache-max-mb-iri
  (system-iri "cachMaxMb"))

(def commit-storage-iri
  (system-iri "commitStorage"))

(def index-storage-iri
  (system-iri "indexStorage"))

(def primary-publisher-iri
  (system-iri "primaryPublisher"))

(def secondary-publishers-iri
  (system-iri "secondaryPublishers"))

(def remote-systems-iri
  (system-iri "remoteSystems"))

(def servers-iri
  (system-iri "servers"))

(def ledger-defaults-iri
  (system-iri "ledgerDefaults"))

(def index-options-iri
  (system-iri "indexOptions"))

(def reindex-min-bytes-iri
  (system-iri "reindexMinBytes"))

(def reindex-max-bytes-iri
  (system-iri "reindexMaxBytes"))

(def max-old-indexes-iri
  (system-iri "maxOldIndexes"))

(def ledger-directory-iri
  (system-iri "ledgerDirectory"))

(def log-directory-iri
  (system-iri "logDirectory"))

(def log-history-iri
  (system-iri "logHistory"))

(def entries-max-iri
  (system-iri "entriesMax"))

(def catch-up-rounds-iri
  (system-iri "catchUpRounds"))

(def this-server-iri
  (system-iri "thisServer"))

(def max-pending-txns-iri
  (system-iri "maxPendingTxns"))

(def connection-iri
  (system-iri "connection"))

(defn type?
  [node kind]
  (-> node (get-first :type) (= kind)))

(defn connection?
  [node]
  (type? node connection-type))

(defn system?
  [node]
  (type? node system-type))

(defn consensus?
  [node]
  (type? node consensus-type))

(defn raft-consensus?
  [node]
  (and (consensus? node)
       (-> node (get-first-value consensus-protocol-iri) (= "raft"))))

(defn standalone-consensus?
  [node]
  (and (consensus? node)
       (-> node (get-first-value consensus-protocol-iri) (= "standalone"))))

(defn http-api?
  [node]
  (and (type? node api-type)
       (contains? node http-port-iri)))

(defn jetty-api?
  [node]
  (http-api? node))

(defn publisher?
  [node]
  (type? node publisher-type))

(defn storage-nameservice?
  [node]
  (and (publisher? node)
       (contains? node storage-iri)))

(defn ipns-nameservice?
  [node]
  (and (publisher? node)
       (contains? node ipfs-endpoint-iri)
       (contains? node ipns-profile-iri)))

(defn storage?
  [node]
  (type? node storage-type))

(defn memory-storage?
  [node]
  (and (storage? node)
       (-> node
           (dissoc :idx :id :type address-identifier-iri)
           empty?)))

(defn file-storage?
  [node]
  (and (storage? node)
       (contains? node file-path-iri)))

(defn s3-storage?
  [node]
  (and (storage? node)
       (contains? node s3-bucket-iri)))

(defn ipfs-storage?
  [node]
  (and (storage? node)
       (contains? node ipfs-endpoint-iri)))

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

(defmethod ig/expand-key :fluree.server/http
  [k config]
  (let [max-txn-wait-ms (get-first config max-txn-wait-ms-iri)
        config*         (-> config
                            (assoc :handler (ig/ref :fluree.server.api/handler))
                            (dissoc max-txn-wait-ms-iri))]
    {k                      config*
     :fluree.server/watcher {:max-txn-wait-ms max-txn-wait-ms}}))

(defmethod ig/expand-key :fluree.server.consensus/standalone
  [k config]
  {k (assoc config
            :subscriptions (ig/ref :fluree.server/subscriptions)
            :watcher (ig/ref :fluree.server/watcher))})

(defmethod ig/init-key :fluree.server.storage/memory
  [_ config]
  (let [identifier (get-first config address-identifier-iri)]
    (memory-storage/open identifier)))

(defmethod ig/init-key :fluree.server.storage/file
  [_ config]
  (let [identifier (get-first config address-identifier-iri)
        root-path  (get-first config file-path-iri)]
    (file-storage/open identifier root-path)))

(defmethod ig/init-key :fluree.server.storage/s3
  [_ config]
  (let [identifier  (get-first config address-identifier-iri)
        s3-bucket   (get-first config s3-bucket-iri)
        s3-prefix   (get-first config s3-prefix-iri)
        s3-endpoint (get-first config s3-endpoint-iri)]
    (s3-storage/open identifier s3-bucket s3-prefix s3-endpoint)))

(defmethod ig/init-key :fluree.server.storage/ipfs
  [_ config]
  (let [identifier    (get-first config address-identifier-iri)
        ipfs-endpoint (get-first config ipfs-endpoint-iri)]
    (ipfs-storage/open identifier ipfs-endpoint)))

(defmethod ig/init-key :fluree.server.nameservice/storage
  [_ config]
  (let [storage (get-first config storage-iri)]
    (storage-nameservice/start storage)))

(defmethod ig/init-key :fluree.server/remote-system
  [_ config]
  (let [servers     (get config servers-iri)
        identifiers (get config address-identifiers-iri)]
    (remote-system/connect servers identifiers)))

(defmethod ig/init-key :fluree.server/connection
  [_ config]
  (let [cache-max-mb         (get-first-value config cache-max-mb-iri)
        parallelism          (get-first-value config parallelism-iri)
        primary-publisher    (get-first config primary-publisher-iri)
        secondary-publishers (get config secondary-publishers-iri)
        remote-systems       (get config remote-systems-iri)
        ledger-defaults      (get-first config ledger-defaults-iri)
        index-options        (get-first ledger-defaults index-options-iri)
        reindex-min-bytes    (get-first index-options reindex-min-bytes-iri)
        reindex-max-bytes    (get-first index-options reindex-max-bytes-iri)
        max-old-indexes      (get-first index-options max-old-indexes-iri)
        ledger-defaults*     {:index-options {:reindex-min-bytes reindex-min-bytes
                                              :reindex-max-bytes reindex-max-bytes
                                              :max-old-indexes   max-old-indexes}}]
    (connection/connect {:parallelism          parallelism
                         :cache-max-mb         cache-max-mb
                         :primary-publisher    primary-publisher
                         :secondary-publishers secondary-publishers
                         :remote-systems       remote-systems
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
  (let [log-history      (get-first-value config log-history-iri)
        entries-max      (get-first-value config entries-max-iri)
        catch-up-rounds  (get-first-value config catch-up-rounds-iri)
        servers          (get-first-value config servers-iri)
        this-server      (get-first-value config this-server-iri)
        log-directory    (get-first-value config log-directory-iri)
        ledger-directory (get-first-value config ledger-directory-iri)]
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
  (let [max-pending-txns (get-first-value config max-pending-txns-iri)
        conn             (get-first config connection-iri)]
    (standalone/start conn subscriptions watcher max-pending-txns)))

(defmethod ig/halt-key! :fluree.server.consensus/standalone
  [_ transactor]
  (standalone/stop transactor))

(defmethod ig/init-key :fluree.server.api/handler
  [_ {:keys [connection consensus watcher subscriptions]}]
  (handler/app connection consensus watcher subscriptions))

(defmethod ig/init-key :fluree.server.http/jetty
  [_ {:keys [handler] :as config}]
  (let [port (get-first-value config http-port-iri)]
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

(def default-resource-name "config.jsonld")

(def base-config
  {:fluree.server/subscriptions {}
   :fluree.server.api/handler   {:connection    (ig/ref :fluree.server/connection)
                                 :consensus     (ig/ref :fluree.server/consensus)
                                 :watcher       (ig/ref :fluree.server/watcher)
                                 :subscriptions (ig/ref :fluree.server/subscriptions)}})

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
  [config]
  (-> config json-ld/expand parse ig/expand ig/init))

(defn start-file
  ([path]
   (start-file path :prod))
  ([path profile]
   (-> path
       (config/load-file profile)
       start-config)))

(defn start-resource
  ([resource-name]
   (start-resource resource-name :prod))
  ([resource-name profile]
   (-> resource-name
       (config/load-resource profile)
       start-config)))

(def start
  (partial start-resource default-resource-name))

(defn stop
  [server]
  (ig/halt! server))
