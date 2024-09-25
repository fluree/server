(ns fluree.server.system
  (:require [clojure.string :as str]
            [fluree.db.api :as fluree]
            [fluree.db.json-ld.iri :as iri]
            [fluree.db.storage.file :as file-storage]
            [fluree.db.storage.ipfs :as ipfs-storage]
            [fluree.db.storage.memory :as memory-storage]
            [fluree.db.storage.s3 :as s3-storage]
            [fluree.db.util.core :as util]
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

(derive :fluree.consensus/raft :fluree/consensus)
(derive :fluree.consensus/standalone :fluree/consensus)

(derive :fluree.storage/file :fluree/content-storage)
(derive :fluree.storage/file :fluree/byte-storage)
(derive :fluree.storage/file :fluree/json-archive)

(derive :fluree.storage/memory :fluree/content-storage)
(derive :fluree.storage/memory :fluree/byte-storage)
(derive :fluree.storage/memory :fluree/json-archive)

(derive :fluree.storage/s3 :fluree/content-storage)
(derive :fluree.storage/s3 :fluree/byte-storage)
(derive :fluree.storage/s3 :fluree/json-archive)

(derive :fluree.storage/ipfs :fluree/content-storage)
(derive :fluree.storage/ipfs :fluree/json-archive)

(derive :fluree/remote-resources :fluree/json-archive)
(derive :fluree/remote-resources :fluree/nameservice)
(derive :fluree/remote-resources :fluree/publication)

(derive :fluree.nameservice/storage-backed :fluree/nameservice)
(derive :fluree.nameservice/storage-backed :fluree/publisher)

(derive :fluree.nameservice/ipns :fluree/nameservice)
(derive :fluree.nameservice/ipns :fluree/publisher)

(derive :fluree.http/jetty :fluree/http)

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

(def consensus-type
  (system-iri "Consensus"))

(def api-type
  (system-iri "API"))

(def address-identifier-iri
  (system-iri "addressIdentifier"))

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

(defn type?
  [node kind]
  (-> node (get "@type") (= kind)))

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
       (-> node (get consensus-protocol-iri) (= "raft"))))

(defn standalone-consensus?
  [node]
  (and (consensus? node)
       (-> node (get consensus-protocol-iri) (= "standalone"))))

(defn http-api?
  [node]
  (and (type? node api-type)
       (contains? node http-port-iri)))

(defn publisher?
  [node]
  (type? node publisher-type))

(defn storage-backed-nameservice?
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
           (dissoc "@id" "@type" address-identifier-iri)
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
  (get node "@id"))

(defn derive-node-id
  [node]
  (let [id (get-id node)]
    (cond
      (connection? node)                 (derive id :fluree/connection)
      (system? node)                     (derive id :fluree/remote-resources)
      (raft-consensus? node)             (derive id :fluree.consensus/raft)
      (standalone-consensus? node)       (derive id :fluree.consensus/standalone)
      (http-api? node)                   (derive id :fluree.http/jetty) ; TODO: Enable other http servers
      (memory-storage? node)             (derive id :fluree.storage/memory)
      (file-storage? node)               (derive id :fluree.storage/file)
      (s3-storage? node)                 (derive id :fluree.storage/s3)
      (ipfs-storage? node)               (derive id :fluree.storage/ipfs)
      (ipns-nameservice? node)           (derive id :fluree.nameservice/ipns)
      (storage-backed-nameservice? node) (derive id :fluree.nameservice/storage-backed))
    node))

(defn flatten-node
  [node]
  (loop [[[k v] & r] node
         children    []
         flat-node   {}]
    (if k
      (if (map? v)
        (if (contains? v "@id")
          (let [children*  (if (> (count v) 1)
                             (conj children v)
                             children)
                ref-node   (select-keys v ["@id"])
                flat-node* (assoc flat-node k ref-node)]
            (recur r children* flat-node*))
          (let [id         (iri/new-blank-node-id)
                v*         (assoc v "@id" id)
                children*  (conj children v*)
                ref-node   {"@id" id}
                flat-node* (assoc flat-node k ref-node)]
            (recur r children* flat-node*)))
        (let [flat-node* (assoc flat-node k v)]
          (recur r children flat-node*)))
      [flat-node children])))

(defn flatten-nodes
  [nodes]
  (loop [remaining nodes
         flattened []]
    (if-let [node (peek remaining)]
      (let [[flat-node children] (flatten-node node)
            remaining* (-> remaining
                           pop
                           (into children))
            flattened* (conj flattened flat-node)]
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
  (->> iri
       (or (iri/new-blank-node-id))
       iri/decompose
       (map kw-encode)
       (apply keyword)))

(defn keywordize-node-id
  [node]
  (if (map? node)
    (update node "@id" iri->kw)
    node))

(defn keywordize-child-ids
  [node]
  (into {}
        (map (fn [k v]
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
       (contains? node "@id")
       (-> node (dissoc "@id") empty?)))

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

(def default-resource-name "config.jsonld")

(defn parse-config
  [config]
  (let [config*      (->> config json-ld/expand util/sequential first)
        system-graph (-> config* (get "@graph") util/sequential)]
    (->> system-graph
         flatten-nodes
         (map keywordize-node-ids)
         (map derive-node-id)
         (map convert-references)
         (map (juxt get-id identity))
         (into {}))))

(defmethod ig/init-key :fluree.storage/memory
  [_ config]
  (let [identifier (get config address-identifier-iri)]
    (memory-storage/open identifier)))

(defmethod ig/init-key :fluree.storage/file
  [_ config]
  (let [identifier (get config address-identifier-iri)
        root-path  (get config file-path-iri)]
    (file-storage/open identifier root-path)))

(defmethod ig/init-key :fluree.storage/s3
  [_ config]
  (let [identifier  (get config address-identifier-iri)
        s3-bucket   (get config s3-bucket-iri)
        s3-prefix   (get config s3-prefix-iri)
        s3-endpoint (get config s3-endpoint-iri)]
    (s3-storage/open identifier s3-bucket s3-prefix s3-endpoint)))

(defmethod ig/init-key :fluree.storage/ipfs
  [_ config]
  (let [identifier (get config address-identifier-iri)
        ipfs-endpoint (get config ipfs-endpoint-iri)]
    (ipfs-storage/open identifier ipfs-endpoint)))

(defmethod ig/init-key :fluree/subscriptions
  [_ _]
  (subscriptions/listen))

(defmethod ig/halt-key! :fluree/subscriptions
  [_ subsc]
  (subscriptions/close subsc))

(defmethod ig/init-key :fluree/watcher
  [_ {:keys [max-txn-wait-ms]}]
  (watcher/watch max-txn-wait-ms))

(defmethod ig/halt-key! :fluree/watcher
  [_ watcher]
  (watcher/close watcher))

(defmethod ig/init-key :fluree.consensus/raft
  [_ {:keys [server] :as config}]
  (let [config* (assoc config :ledger-directory (:storage-path server))]
    (raft/start config*)))

(defmethod ig/halt-key! :fluree.consensus/raft
  [_ {:keys [close] :as _raft-group}]
  (close))

(defmethod ig/init-key :fluree.consensus/standalone
  [_ {:keys [conn subscriptions watcher max-pending-txns]}]
  (standalone/start conn subscriptions watcher max-pending-txns))

(defmethod ig/halt-key! :fluree.consensu/standalone
  [_ transactor]
  (standalone/stop transactor))

(defmethod ig/init-key :fluree.api/handler
  [_ {:keys [conn consensus watcher subscriptions]}]
  (handler/app conn consensus watcher subscriptions))

(defmethod ig/init-key :fluree.http/jetty
  [_ {:keys [handler port join?]}]
  (jetty/run-jetty handler {:port port, :join? join?}))

(defmethod ig/halt-key! :fluree.http/jetty
  [_ http-server]
  (jetty/stop-server http-server))

(defmethod ig/init-key :fluree/sid-migration
  [_ {:keys [conn ledgers force]}]
  (task.migrate-sid/migrate conn ledgers force))

(defmethod ig/init-key :default
  [_ config]
  config)

(defn start-config
  [config]
  (-> config parse-config ig/init))

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
