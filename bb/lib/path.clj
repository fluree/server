(ns lib.path
  (:require [clojure.string :as str]))

(defn tmp->project-rel
  "Takes a temp root dir and full temp path and removes the temp root prefix so
  that the returned path is relative to the project."
  [tmp-root tmp-path]
  (-> tmp-path
      str/join
      (str/replace "../" "")
      (str/replace #"^[^/]" "/")
      (str/replace-first tmp-root "")))
