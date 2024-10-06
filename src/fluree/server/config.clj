(ns fluree.server.config
  (:refer-clojure :exclude [load-file])
  (:require [camel-snake-kebab.core :refer [->kebab-case-keyword]]
            [clojure.java.io :as io]
            [fluree.db.util.json :as json]
            [fluree.server.config.validation :as validation]
            [fluree.server.config.env :as env]))

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
      validation/coerce
      with-namespaced-keys))

(defn parse-config
  [cfg]
  (json/parse cfg false))

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
   (read-resource resource-name)))

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
   (read-file path)))
