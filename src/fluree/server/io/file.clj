(ns fluree.server.io.file
  (:require [clojure.core.async :as async]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [fluree.db.util.log :as log]
            [fluree.server.io.crypto :as crypto]
            [robert.bruce :refer [try-try-again]])
  (:import (java.io ByteArrayOutputStream File FileNotFoundException)))

(set! *warn-on-reflection* true)

(defn canonicalize-path
  "Ensures that `path` arg starts with either `./` or `/`. If it starts with
  any other character, `./` will be prepended to the returned string. Otherwise
  the original arg will be returned."
  [path]
  (if (boolean (re-find #"^[./]" path))
    path
    (str "./" path)))

(defn key->unix-path
  "Given an optional base-path and our key, returns the storage path as a
  UNIX-style `/`-separated path.

  TODO: We need to get rid of the encoding form on the key... for sure!"
  ([key] (key->unix-path nil key))
  ([base-path key]
   (let [key       (if (or (str/ends-with? key ".avro")
                           (str/ends-with? key ".snapshot")
                           (str/ends-with? key ".jsonld"))
                     key
                     (str key ".fdbd"))
         split-key (str/split key #"_")
         file      (apply io/file base-path split-key)]
     (.toString ^File file))))

(defn read-file
  "Returns nil if file does not exist."
  [path]
  (let [read-fn (fn [p]
                  (try
                    (with-open [xin  (io/input-stream p)
                                xout (ByteArrayOutputStream.)]
                      (io/copy xin xout)
                      (.toByteArray xout))
                    (catch FileNotFoundException _
                      nil)
                    (catch Exception e
                      (log/error e "Failed to read file at path" path
                                 ". Retrying.")
                      (throw e))))]
    ;; Filestore reads can sometimes fail transiently for large databases
    (try-try-again {:sleep 100, :tries 3} read-fn path)))

(defn storage-read
  [base-path key]
  (log/trace "Storage read: " {:base-path base-path :key key})
  (->> key
       (key->unix-path base-path)
       read-file))

(defn connection-storage-read
  "Default function for connection storage."
  ([base-path] (connection-storage-read base-path nil))
  ([base-path encryption-key]
   (if encryption-key
     (fn [key]
       (async/thread (let [data (storage-read base-path key)]
                       (when data (crypto/decrypt-bytes data encryption-key)))))
     (fn [key]
       (async/thread (storage-read base-path key))))))

(defn write-file
  [^bytes val path]
  (try
    (with-open [out (io/output-stream (io/file path))]
      (.write out val))
    (catch FileNotFoundException _
      (try
        (io/make-parents (io/file path))
        (with-open [out (io/output-stream (io/file path))]
          (.write out val))
        (catch Exception e
          (log/error (str "Unable to create storage directory: " path
                          " with error: " (.getMessage e) ". Permission issue?"))
          (log/error (str "Fatal Error, shutting down!"))
          (System/exit 1))))
    (catch Exception e (throw e))))

(defn exists?
  [path]
  (.exists (io/file path)))

(defn safe-delete
  [path]
  (if (exists? path)
    (try
      (clojure.java.io/delete-file path)
      true
      (catch Exception e (str "exception: " (.getMessage e))))
    false))

(defn storage-write
  [base-path key data]
  (log/trace "Storage write: " {:base-path base-path :key key :data data})
  (if (nil? data)
    (->> key
         (key->unix-path base-path)
         (safe-delete))
    (->> key
         (key->unix-path base-path)
         (write-file data))))

(defn storage-write-async
  [base-path key data]
  (async/thread
    (if (str/ends-with? key ".jsonld")
      ;; for new json-ld, key is base-32 of hash... store in sub-directories of first 3 chars to avoid large directories
      (storage-write base-path (str (subs key 0 3) "_" key) data)
      (storage-write base-path key data))))

(defn connection-storage-write
  "Default function for connection storage writing."
  ([base-path] (connection-storage-write base-path nil))
  ([base-path encryption-key]
   (let [str->bytes (fn [x] (if (string? x)
                              (.getBytes ^String x)
                              x))]
     (if encryption-key
       (fn [key data]
         (->> (crypto/encrypt-bytes (str->bytes data) encryption-key)
              (storage-write-async base-path key)))
       (fn [key data]
         (->> (str->bytes data)
              (storage-write-async base-path key)))))))

(defn storage-exists?
  [base-path key]
  (->> key
       (key->unix-path base-path)
       exists?))

(defn connection-storage-exists?
  "Default function for connection storage."
  [base-path]
  (fn [key]
    (async/thread
      (storage-exists? base-path key))))

(defn storage-delete
  [base-path key]
  (let [path (key->unix-path base-path key)]
    (safe-delete path)))

(defn connection-storage-delete
  [base-path]
  (fn [key]
    (async/thread
      (storage-delete base-path key))))

(defn storage-list
  "Returns a sequence of data maps with keys `#{:name :size :url}` representing
  the files at `base-path/path`."
  [base-path path]
  (let [full-path (str base-path path)]
    (log/debug "storage-list full-path:" full-path)
    (when (exists? full-path)
      (let [files (-> full-path io/file file-seq)]
        (map (fn [^File f] {:name (.getName f), :url (.toURI f), :size (.length f)})
             files)))))

(defn connection-storage-list
  [base-path]
  (fn [path]
    (async/thread
      (storage-list base-path path))))