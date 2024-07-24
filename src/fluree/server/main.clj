(ns fluree.server.main
  (:require [clojure.tools.cli :as cli]
            [clojure.string :as str]
            [fluree.db.util.log :as log]
            [fluree.server.system :as system])
  (:gen-class))

(set! *warn-on-reflection* true)

(def options
  [["-p" "--profile PROFILE" "Run profile"
    :parse-fn keyword]
   ["-c" "--config FILE" "Configuration file path"]
   ["-h" "--help" "Print this usage summary and exit"]])

(defn usage
  [summary]
  (str/join \newline ["Fluree Ledger Server"
                      ""
                      "Options:"
                      summary]))

(defn error-message
  [errors]
  (str/join \newline errors))

(defn exit
  [status message]
  (println message)
  (System/exit status))

(defn start-server
  [{:keys [profile] :as _options}]
  (if-let [config-path (:config options)]
    (do (log/info "Starting Fluree server configuration at path:" config-path
                  "with profile:" profile)
        (system/start-file config-path profile))
    (do (log/info "Starting Fluree server with profile:" profile)
        (system/start profile))))

(defn -main
  [& args]
  (let [{:keys [options errors summary]} (cli/parse-opts args options)]
    (cond (seq errors)
          (let [msg (error-message errors)]
            (exit 1 msg))

          (:help options)
          (let [msg (usage summary)]
            (exit 0 msg))

          :else
          (start-server options))))
