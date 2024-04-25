(ns lib.cljfmt
  (:require [babashka.fs]
            [cljfmt.tool :as fmt]
            [cljfmt.report :as report]
            [clojure.string :as str]
            [lib.path :as path]))

(defn- nil-or-zero?
  [v]
  (or (nil? v) (zero? v)))

(defn check
  "Runs cljfmt check on all files (recursively) in dir. Returns a collection of
  any files that failed the check."
  [dir]
  (let [cljfmt-opts {:paths [dir], :diff? false, :report report/clojure}
        {:keys [counts incorrect error] :as _result} (fmt/check cljfmt-opts)]
    (if (and (nil-or-zero? (:incorrect counts)) (nil-or-zero? (:error counts)))
      []
      (let [->proj-path (partial path/tmp->project-rel
                                 (if (str/ends-with? dir "/")
                                   dir
                                   (str dir "/")))]
        (concat (map (fn [[file]] (->proj-path file)) incorrect)
                (map (fn [[file]] (->proj-path file)) error))))))

(def clj-file-re ".*\\.(clj[sc]?|edn|bb)")

(defn fix
  "Runs cljfmt fix on all CLJ-related files (recursively) in dir."
  [dir]
  ;; We have to do this match ourselves b/c o/w cljfmt will pick up files in
  ;; the .git dir that have right filename extension, but do not contain valid
  ;; Clojure.
  (let [clj-paths (fs/match dir (str "regex:" clj-file-re) {:recursive true})]
    (fmt/fix-paths (map fs/file clj-paths))))
