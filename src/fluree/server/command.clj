(ns fluree.server.command
  (:require [clojure.string :as str]
            [clojure.tools.cli :as cli]))

(set! *warn-on-reflection* true)

(def cli-options
  [["-p" "--profile PROFILE" "Run profile"]
   ["-c" "--config FILE" "Load configuration at a file path"]
   ["-s" "--string STRING" "Load stringified configuration"]
   ["-r" "--resource NAME" "Load pre-defined configuration resource"]
   ["--reindex" "--reindex LEDGER" "Reindex the specified ledger"]
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

(defn parse
  [args]
  (-> args
      (cli/parse-opts cli-options)
      validate-opts))

(defn verify
  [{:keys [errors] :as cli}]
  (if (seq errors)
    (let [msg (error-message errors)]
      (exit 1 msg))
    cli))

(defn describe
  [{:keys [options summary] :as cli}]
  (if (:help options)
    (let [msg (usage summary)]
      (exit 0 msg))
    cli))

(defn formulate
  [args]
  (-> args parse verify describe))
