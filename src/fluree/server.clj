(ns fluree.server
  (:require [fluree.db.util.log :as log]
            [fluree.server.command :as command]
            [fluree.server.system :as system])
  (:gen-class))

(set! *warn-on-reflection* true)

(defn start
  [{:keys [options] :as _cli}]
  (let [{:keys [profile reindex]} options]
    (if-let [config-string (:string options)]
      (do (log/info "Starting Fluree server from command line configuration"
                    (when profile (str "with profile: " profile)))
          (system/start-config config-string :profile profile :reindex reindex))
      (if-let [config-path (:config options)]
        (do (log/info "Starting Fluree server from configuration file at path:" config-path
                      (when profile (str "with profile: " profile)))
            (system/start-file config-path :profile profile :reindex reindex))
        (if-let [config-resource (:resource options)]
          (do (log/info "Starting Fluree server from configuration resource:" config-resource
                        (when profile (str "with profile: " profile)))
              (system/start-resource config-resource :profile profile :reindex reindex))
          (do (log/info "Starting Fluree server with default configuration:"
                        system/default-resource-name
                        (when profile (str "with profile: " profile)))
              (system/start :profile profile :reindex reindex)))))))

(defn -main
  [& args]
  (-> args command/formulate start))
