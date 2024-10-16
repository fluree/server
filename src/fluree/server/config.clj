(ns fluree.server.config
  (:refer-clojure :exclude [load-file])
  (:require [clojure.string :as str]
            [clojure.java.io :as io]
            [fluree.db.connection.vocab :as conn-vocab]
            [fluree.db.json-ld.iri :as iri]
            [fluree.db.util.core :as util :refer [get-id  get-first-value]]
            [fluree.db.util.json :as json]
            [fluree.json-ld :as json-ld]
            [fluree.server.config.env :as env]
            [fluree.server.config.validation :as validation]
            [fluree.server.config.vocab :as vocab]))

(set! *warn-on-reflection* true)

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
        env-overrides     (env/config)]
    (-> config
        (dissoc :profiles)
        (deep-merge profile-overrides env-overrides))))

(defn consensus?
  [node]
  (conn-vocab/type? node vocab/consensus-type))

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
  (and (conn-vocab/type? node vocab/api-type)
       (contains? node vocab/http-port)))

(defn jetty-api?
  [node]
  (http-api? node))

(defn derive-node-id
  [node]
  (let [id (get-id node)]
    (cond
      (raft-consensus? node)       (derive id :fluree.server.consensus/raft)
      (standalone-consensus? node) (derive id :fluree.server.consensus/standalone)
      (jetty-api? node)            (derive id :fluree.server.http/jetty) ; TODO: Enable other http servers
      :else                        (conn-vocab/derive-node-id node))
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

(defn finalize
  [config profile]
  (-> config
      (apply-overrides profile)
      validation/coerce))

(def base-config
  {:fluree.server/subscriptions {}})

(defn parse
  [cfg]
  (let [cfg* (if (string? cfg)
               (json/parse cfg false)
               cfg)]
    (->> cfg*
         json-ld/expand
         util/sequential
         flatten-nodes
         (map keywordize-node-ids)
         (map derive-node-id)
         (map (juxt get-id identity))
         (into base-config))))

(defn load-resource
  [resource-name]
  (-> resource-name
      io/resource
      slurp
      parse))

(defn load-file
  [path]
  (-> path
      io/file
      slurp
      parse))
