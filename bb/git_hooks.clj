(ns git-hooks
  (:require [babashka.fs :as fs]
            [lib.git :as git]
            [lint])
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
      (lint/check :cljfmt staging-dir)
      (lint/check :clj-kondo staging-dir))
    (println)
    (println "🤘 Commit looks good! 🤘")))

(defmethod hook :default
  [& args]
  (println "Unknown git hook:" (first args)))
