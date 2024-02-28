(ns fluree.server.consensus.handlers.new-index-file
  (:require [clojure.core.async :as async]
            [clojure.string :as str]
            [fluree.db.util.filesystem :as fs]
            [fluree.db.util.log :as log]
            [fluree.server.consensus.core :as consensus]))

(set! *warn-on-reflection* true)

(defn address-path
  [address]
  (let [[_ _ path] (str/split address #":")]
    path))
(defn address-full-path
  [{:keys [storage-path] :as _conn} address]
  (str (fs/local-path storage-path) "/" (address-path address)))

(defn processor
  "Stores a new index file at the specified address.

  Only stores if the server is not the current server"
  [{:keys [consensus/raft-state fluree/conn] :as _config}
   {:keys [server address data] :as _params}]
  (future
    (try
      (if (= server (consensus/this-server raft-state))
        (log/debug "Consensus: new index file originated from this server, not writing: " address)
        (let [[_ path] (re-matches #"^fluree:file://(.+)$" address)
              local-path (address-full-path conn path)]
          (if path
            (async/<!! (fs/write-file local-path (.getBytes ^String (:json data))))
            (log/error "Consensus: Cannot write new index file, not a file path address: " address))))
      (catch Exception e
        (log/error e "Consensus: Unexpected error writing new index file: " (ex-message e))))))

(defn handler
  "Does a no-op and returns params untouched for handling by the processor.

  We don't update raft state currently based on receiving an index file, just
  need to asynchronously store the file once received."
  [_ params]
  (log/trace "Consensus: New index file received: " params)
  params)
