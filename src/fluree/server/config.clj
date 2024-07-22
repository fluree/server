(ns fluree.server.config
  (:require [camel-snake-kebab.core :refer [->kebab-case-keyword]]
            [clojure.java.io :as io]
            [fluree.db.util.json :as json]
            [malli.core :as m]))

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
   (-> resource-name
       read-resource
       (dissoc :profiles)))

  ([resource-name profile]
   (let [config    (read-resource resource-name)
         overrides (get-in config [:profiles profile])]
     (-> config
         (dissoc :profiles)
         (deep-merge overrides)))))
