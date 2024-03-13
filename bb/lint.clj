(ns lint
  (:require [babashka.fs :as fs]
            [clojure.edn :as edn]
            [clojure.string :as str]
            [lib.clj-kondo :as clj-kondo]
            [lib.cljfmt :as cljfmt]))

(defn- check-enabled?
  [check]
  (let [skip-checks      (System/getenv "SKIP_CHECKS")
        skip-checks-coll (when skip-checks
                           (edn/read-string
                            (if (str/starts-with? skip-checks "[")
                              skip-checks
                              (str "[" skip-checks "]"))))
        disabled-checks  (into #{} (map keyword) skip-checks-coll)]
    (not (contains? disabled-checks check))))

(defmulti check (fn [& args] (-> args first keyword)))

(defmethod check :cljfmt
  [_ staging-dir]
  (let [bad-files (cljfmt/check staging-dir)]
    (when (and (check-enabled? :cljfmt) (seq bad-files))
      (println)
      (println (let [bfc (count bad-files)]
                 (str bfc " " (if (= 1 bfc) "file is" "files are")
                      " formatted incorrectly:\n"
                      (str/join "\n" bad-files)
                      "\n")))
      (println "Run: cljfmt fix" (str/join " " bad-files))
      (println "     git add" (str/join " " bad-files))
      (println)
      (System/exit 1))))

(defmethod check :clj-kondo
  [_ staging-dir]
  (let [bad-files (clj-kondo/lint staging-dir)]
    (when (and (check-enabled? :clj-kondo) (seq bad-files))
      (println)
      (println "Fix errors and run: git add" (str/join " " bad-files))
      (System/exit 2))))

(defn run-all
  "Runs all enabled linters at root `dir`"
  [dir]
  (let [path (-> dir fs/file fs/canonicalize str)]
    (println "Linting" path)
    (check :cljfmt path)
    (check :clj-kondo path)))
