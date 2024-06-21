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
  [_ dir]
  (when (check-enabled? :cljfmt)
    (let [bad-files (cljfmt/check dir)]
      (when (seq bad-files)
        (println)
        (println (let [bfc (count bad-files)]
                   (str bfc " " (if (= 1 bfc) "file is" "files are")
                        " formatted incorrectly:\n"
                        (str/join "\n" bad-files)
                        "\n")))
        (println "Run: cljfmt fix" (str/join " " bad-files))
        (println "     git add" (str/join " " bad-files))
        (println)
        (System/exit 1)))))

(defmethod check :clj-kondo
  [_ dir]
  (when (check-enabled? :clj-kondo)
    (let [bad-files (clj-kondo/lint dir)]
      (when (seq bad-files)
        (println)
        (println "Fix errors and run: git add" (str/join " " bad-files))
        (System/exit 2)))))

(defmulti fix (fn [& args] (-> args first keyword)))

(defmethod fix :cljfmt
  [_ dir]
  (when (check-enabled? :cljfmt)
    (cljfmt/fix dir)))

(defmethod fix :clj-kondo
  [_ _]
  ;; clj-kondo doesn't support fixing the issues it identifies
  true)

(defn check-all
  "Runs all enabled linters at root `dir`"
  [dir]
  (let [path (-> dir fs/file fs/canonicalize str)]
    (println "Linting" path)
    (check :cljfmt path)
    (check :clj-kondo path)))

(defn fix-all
  "Runs all linters in 'fix' mode at root `dir`"
  [dir]
  (let [path (-> dir fs/file fs/canonicalize str)]
    (println "Fixing" path)
    (let [results (fix :cljfmt path)
          fixed   (filter :reformatted results)]
      (println "Reformatted" (count fixed) "file(s):")
      (doseq [file fixed]
        (println (-> file :file str))))))
