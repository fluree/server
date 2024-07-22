(ns fluree.server.config
  (:require [camel-snake-kebab.core :refer [->kebab-case-keyword]]
            [clojure.java.io :as io]
            [fluree.db.util.json :as json]
            [malli.core :as m]))

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
   (-> resource-name
       read-resource
       (update :profiles get profile))))
