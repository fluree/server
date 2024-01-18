(ns git-hooks
  (:require [babashka.fs :as fs]
            [clojure.edn :as edn]
            [clojure.string :as str]
            [lib.clj-kondo :as clj-kondo]
            [lib.cljfmt :as cljfmt]
            [lib.git :as git])
  (:import (java.util Date)))

;; originally inspired by https://blaster.ai/blog/posts/manage-git-hooks-w-babashka.html

;; installation

(defn hook-script
  [hook]
  (format "#!/bin/sh
# Installed by babashka task on %s

bb git-hooks %s" (Date.) hook))

(defn spit-hook
  [hook]
  (println "Installing git hook:" hook)
  (let [file (str ".git/hooks/" hook)]
    (spit file (hook-script hook))
    (fs/set-posix-file-permissions file "rwx------")
    (assert (fs/executable? file))))

(defmulti install-hook (fn [& args] (-> args first keyword)))

(defmethod install-hook :pre-commit
  [& _]
  (spit-hook "pre-commit"))

(defmethod install-hook :default
  [& args]
  (println "Unknown git hook:" (first args)))

;; checks

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

;; hooks

(defmulti hook (fn [& args] (-> args first keyword)))

(defmethod hook :install
  [& _]
  (install-hook :pre-commit))

(defmethod hook :pre-commit
  [& _]
  (println "Running pre-commit hook")
  (when-let [files (git/changed-files)]
    (println "Found" (count files) "changed files\n")
    (let [staging-dir (git/staged-contents files)]
      (check :cljfmt staging-dir)
      (check :clj-kondo staging-dir))
    (println)
    (println "🤘 Commit looks good! 🤘")))

(defmethod hook :default
  [& args]
  (println "Unknown git hook:" (first args)))
