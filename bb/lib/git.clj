(ns lib.git
  (:require [babashka.fs :as fs]
            [clojure.java.shell :refer [sh]]
            [clojure.string :as str]))

(defn changed-files
  "Returns the filenames of all changed files currently staged in git.
  NB: This is not the same as what will be committed. Git allows parts of files
  to be staged while other parts are left un-staged. So the content of these files
  in your local working copy won't always be the same as what would be committed
  from that index. You probably want to pass the return value of this fn to the
  `staged-contents` fn before running any hooks."
  []
  (->> (sh "git" "diff" "--cached" "--name-only" "--diff-filter=ACM")
       :out
       str/split-lines
       (filter seq)
       seq))

(defn staged-contents
  "Returns the path to a temp dir with the staged contents of the files
  argument. This is necessary because git allows partial staging of files."
  [files]
  (let [tmp-dir (str (fs/create-temp-dir))]
    #_(println "staging dir:" tmp-dir)
    (sh "git" "checkout-index" (str "--prefix=" tmp-dir "/") "--stdin"
        :in (str/join "\n" files))
    tmp-dir))
