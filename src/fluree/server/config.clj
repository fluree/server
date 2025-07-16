(ns fluree.server.config
  (:require [clojure.java.io :as io]
            [fluree.db.connection.config :as conn-config]
            [fluree.db.util :as util :refer [get-id  get-first-value]]
            [fluree.db.util.log :as log]
            [fluree.server.config.vocab :as vocab]
            [fluree.server.consensus :as-alias consensus]
            [fluree.server.http :as-alias http]))

(set! *warn-on-reflection* true)

(defn deep-merge
  "Deep merge two maps"
  [& maps]
  (apply merge-with
         (fn [v1 v2]
           (if (and (map? v1) (map? v2))
             (deep-merge v1 v2)
             v2))
         maps))

(defn merge-profile
  "Merge profile overrides into the config's @graph nodes"
  [config profile profile-overrides]
  (let [base-config (dissoc config "profiles")
        graph       (get base-config "@graph")
        ;; Create a map of @id -> override for quick lookup
        overrides   (reduce (fn [m override]
                              (assoc m (get override "@id") override))
                            {}
                            profile-overrides)
        ;; Merge overrides into matching nodes
        merged-graph (mapv (fn [node]
                             (let [node-id (get node "@id")]
                               (if-let [override (get overrides node-id)]
                                 (do
                                   (log/info "Applying profile override for node:" node-id)
                                   (deep-merge node override))
                                 node)))
                           graph)]
    (log/info (str "Applied " (count overrides) " profile overrides from profile: " profile))
    (assoc base-config "@graph" merged-graph)))

(defn apply-profile
  "Apply a profile to a JSON-LD config by merging profile overrides into the @graph nodes"
  [config profile]
  (cond
    ;; No profile specified, just remove profiles section
    (nil? profile)
    (dissoc config "profiles")

    ;; Profile found in config
    (get-in config ["profiles" profile])
    (merge-profile config profile (get-in config ["profiles" profile]))

    ;; Profile not found
    :else
    (do
      (log/warn "Profile" profile "not found in configuration. Available profiles:"
                (keys (get config "profiles")))
      (dissoc config "profiles"))))

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
      (raft-consensus? node)       (derive id ::consensus/raft)
      (standalone-consensus? node) (derive id ::consensus/standalone)
      (jetty-api? node)            (derive id ::http/jetty) ; TODO: Enable other http servers
      :else                        (conn-config/derive-node-id node))
    node))

(defn read-resource
  [resource-name]
  (try
    (-> resource-name io/resource slurp)
    (catch Exception e
      (throw (ex-info (str "Unable to load configuration resource: " resource-name)
                      {:status 400, :error :server/missing-config}
                      e)))))

(defn read-file
  [path]
  (try
    (-> path io/file slurp)
    (catch Exception e
      (throw (ex-info (str "Unable to load configuration file at path: " path)
                      {:status 400, :error :server/missing-config}
                      e)))))

(defn parse
  [cfg]
  (-> cfg
      (conn-config/parse derive-server-node-id)
      (assoc :fluree.server/subscriptions {})))
