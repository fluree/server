(ns fluree.server.main
  (:require [clojure.string :as str]
            [clojure.tools.cli :as cli]
            [fluree.db.util.log :as log]
            [fluree.server.system :as system])
  (:gen-class))

(set! *warn-on-reflection* true)

(defn strip-leading-colon
  [s]
  (if (str/starts-with? s ":")
    (subs s 1)
    s))

(defn profile-string->keyword
  [s]
  (-> s str/trim strip-leading-colon keyword))

(def cli-options
  [["-p" "--profile PROFILE" "Run profile"
    :default  :prod
    :parse-fn profile-string->keyword]
   ["-c" "--config FILE" "Load configuration at a file path"]
   ["-s" "--string STRING" "Load stringified configuration"]
   ["-r" "--resource NAME" "Load pre-defined configuration resource"]
   ["-h" "--help" "Print this usage summary and exit"]])

(defn single-configuration?
  [{:keys [config string resource]}]
  (->> [config string resource] (remove nil?) count (>= 1)))

(def multiple-configuration-error
  (str "Only a single configuration option from"
       "-c/--config, -s/--string, and -r/--resource"
       "is allowed."))

(defn validate-opts
  [{:keys [options] :as parsed-opts}]
  (if (single-configuration? options)
    parsed-opts
    (update parsed-opts :errors conj multiple-configuration-error)))

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
  [{:keys [profile] :as options}]
  (if-let [config-path (:config options)]
    (do (log/info "Starting Fluree server configuration at path:" config-path
                  "with profile:" profile)
        (system/start-file config-path profile))
    (do (log/info "Starting Fluree server with profile:" profile)
        (system/start profile))))

(defn -main
  [& args]
  (let [{:keys [options errors summary]} (-> args
                                             (cli/parse-opts cli-options)
                                             validate-opts)]
    (cond (seq errors)
          (let [msg (error-message errors)]
            (exit 1 msg))

          (:help options)
          (let [msg (usage summary)]
            (exit 0 msg))

          :else
          (start-server options))))
