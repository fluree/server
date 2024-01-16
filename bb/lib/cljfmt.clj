(ns lib.cljfmt
  (:require [cljfmt.lib :as fmt]
            [clojure.string :as str]
            [lib.path :as path]))

(defn check
  "Runs cljfmt check on all files (recursively) in dir. Returns a collection of
  any files that failed the check."
  [dir]
  (let [cljfmt-opts {:paths [dir], :diff? false}
        {:keys [counts incorrect error] :as _result} (fmt/check cljfmt-opts)]
    #_(println "cljfmt results:" (pr-str result))
    (if (and (zero? (:incorrect counts)) (zero? (:error counts)))
      []
      (let [->proj-path (partial path/tmp->project-rel
                                 (if (str/ends-with? dir "/")
                                   dir
                                   (str dir "/")))]
        (concat (map (fn [[file]] (->proj-path file)) incorrect)
                (map (fn [[file]] (->proj-path file)) error))))))
