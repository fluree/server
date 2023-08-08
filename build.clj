(ns build
  (:require [clojure.tools.build.api :as b]
            [org.corfield.build :as bb]))

(def lib 'fluree/server)
(def version (format "0.1.%s" (b/git-count-revs nil)))

(defn uber [_]
      (bb/uber {:lib     lib
                :version version
                :main    'fluree.server.main}))
