(ns lib.clj-kondo
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [lib.path :as path]
            [pod.borkdude.clj-kondo :as clj-kondo]))

(def linted-paths
  ["src" "test" "bb" "build.clj"])

(defn lint
  "Runs clj-kondo --lint on all files (recursively) in dir. Returns a collection
  of any files that failed the check."
  [dir]
  (let [lint-paths (map #(str dir "/" %)
                        (filter #(->> % (io/file dir) .exists) linted-paths))
        result (clj-kondo/run! {:lint lint-paths})
        {{:keys [error warning]} :summary} result]
    (if (and (zero? error) (zero? warning))
      []
      (let [->proj-path (partial path/tmp->project-rel
                                 (if (str/ends-with? dir "/") dir (str dir "/")))
            bad-files   (->> result :findings
                             (map #(update % :filename ->proj-path)))]
        (println "clj-kondo lint:")
        (doseq [{:keys [filename level message row end-row col end-col] :as _bf}
                bad-files]
          (let [row-desc (if (not= row end-row)
                           (str row "-" end-row)
                           (str row))
                col-desc (if (not= col end-col)
                           (str col "-" end-col)
                           (str col))]
            (println (str filename ":" row-desc ":" col-desc ":" level)
                     "-" message)))
        (map :filename bad-files)))))
