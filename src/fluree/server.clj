(ns fluree.server
  (:require [fluree.db.util.log :as log]
            [fluree.server.command :as command]
            [fluree.server.system :as system])
  (:gen-class))

(set! *warn-on-reflection* true)

(defn start
  [{:keys [options] :as _cli}]
  (let [{:keys [profile]} options]
    (if-let [config-string (:string options)]
      (do (log/info "Starting Fluree server from command line configuration with profile:"
                    profile)
          (system/start-config config-string profile))
      (if-let [config-path (:config options)]
        (do (log/info "Starting Fluree server configuration at path:" config-path
                      "with profile:" profile)
            (system/start-file config-path profile))
        (if-let [config-resource (:resource options)]
          (do (log/info "Starting Fluree server configuration from resource:" config-resource)
              (system/start-resource config-resource profile))
          (do (log/info "Starting Fluree server with profile:" profile)
              (system/start profile)))))))

(defn -main
  [& args]
  (-> args command/formulate start))
