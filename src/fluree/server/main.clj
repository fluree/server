(ns fluree.server.main
  (:require [fluree.server.system :as system]
            [fluree.db.util.log :as log])
  (:gen-class))

(set! *warn-on-reflection* true)

(defn -main
  [& args]
  (let [first-arg (first args)
        profile   (or (keyword first-arg)
                      :prod)]
    (log/info "Starting fluree/server with profile:" profile)
    (system/start profile)))
