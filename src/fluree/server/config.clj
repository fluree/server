(ns fluree.server.config
  (:require [fluree.db.connection.config :as conn-config]
            [fluree.db.util.core :as util :refer [get-id  get-first-value]]
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
  (let [profile-overrides (get-in config [:profiles profile])]
    (-> config
        (dissoc :profiles)
        (deep-merge profile-overrides))))

(defn consensus?
  [node]
  (conn-config/type? node vocab/consensus-type))

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
  (and (conn-config/type? node vocab/api-type)
       (contains? node vocab/http-port)))

(defn jetty-api?
  [node]
  (http-api? node))

(defn derive-server-node-id
  [node]
  (let [id (get-id node)]
    (cond
      (raft-consensus? node)       (derive id :fluree.server.consensus/raft)
      (standalone-consensus? node) (derive id :fluree.server.consensus/standalone)
      (jetty-api? node)            (derive id :fluree.server.http/jetty) ; TODO: Enable other http servers
      :else                        (conn-config/derive-node-id node))
    node))

(defn finalize
  [config profile]
  (-> config
      (apply-overrides profile)
      validation/coerce))

(defn parse
  [cfg]
  (-> cfg
      (conn-config/parse (map derive-server-node-id))
      (assoc :fluree.server/subscriptions {})))
