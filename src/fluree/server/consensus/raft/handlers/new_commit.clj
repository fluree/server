(ns fluree.server.consensus.raft.handlers.new-commit
  (:require [clojure.core.async :as async]
            [fluree.db.connection :as connection]
            [fluree.db.storage :as storage]
            [fluree.db.storage.file :as file-storage]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.bytes :as bytes]
            [fluree.db.util.filesystem :as fs]
            [fluree.db.util.json :as json]
            [fluree.db.util.log :as log]
            [fluree.server.consensus.broadcast :as broadcast]))

(set! *warn-on-reflection* true)

(defn write-file
  "Only writes file to disk if address is of type 'file'

  See fluree.db.conn.file namespace for key/vals contained in `file-meta` map."
  [storage-path {:keys [address json] :as _file-meta}]
  (let [{:keys [method]} (storage/parse-address address)]
    (when (= "file" method)
      (let [path       (file-storage/storage-path storage-path address)
            json-bytes (bytes/string->UTF8 json)]
        (async/<!! (fs/write-file path json-bytes))))))

(defn push-nameservice
  [conn {:keys [json address] :as _commit-file-meta}]
  (let [commit-json (-> (json/parse json false)
                        ;; address is not yet written into the commit file, add it
                        (assoc "address" address))]
    (connection/publish-commit conn commit-json)))

(defn store-ledger-files
  "Persist both the data-file and commit-file to disk only if redundant
  local-storage. If using a networked file system (e.g. S3, IPFS) the
  file is already stored by the leader with the respective service."
  [{:keys [consensus/raft-state fluree/conn fluree/server] :as _config}
   {:keys [data-file-meta commit-file-meta] :as commit-result}]
  (go-try
    (let [this-server   (:this-server raft-state)
          commit-server (:server commit-result)
          storage-path  (:storage-path server)]
      (when (not= commit-server this-server)
       ;; if server that created the new ledger is this server, the files
       ;; were already written - only other servers need to write file
        (when data-file-meta ;; if the commit is just being updated, there won't be more data (e.g. after indexing)
          (write-file storage-path data-file-meta))
        (write-file storage-path commit-file-meta)
        (<? (push-nameservice conn commit-file-meta)))
      commit-result)))

(defn update-ledger-state
  "Updates the latest commit in the ledger, and removes the processed transaction in the queue"
  [{:keys [:consensus/state-atom] :as _config}
   {:keys [ledger-id commit-file-meta t tx-id] :as commit-result}]
  (try

    (swap! state-atom
           (fn [current-state]
             (-> current-state
                 ;; remove the transaction from the queue
                 (update-in [:tx-queue ledger-id] dissoc tx-id)
                 ;; add in the new ledger to state
                 (assoc-in [:ledgers ledger-id] {:t              t
                                                 :status         :ready
                                                 :commit (:address commit-file-meta)}))))
    commit-result

    (catch Exception e
      ; return exception (don't throw) for handler on error
      (log/warn (ex-message e))
      (if (ex-data e)
        e
        (ex-info (str "Unexpected error queuing new ledger: " (ex-message e))
                 {:status 500
                  :error  :db/unexpected-error}
                 e)))))

(defn handler
  "Adds a new commit for a ledger into state machine and stores associated files
  if needed."
  [config commit-result]
  (log/debug (str "Queuing new commit into state machine: " commit-result))
  (try

    (->> commit-result
         (store-ledger-files config)
         async/<!!
         (update-ledger-state config))

    (catch Exception e
      (log/warn (str "Error writing new commit: " (ex-message e)))
      (if (ex-data e)
        e
        (ex-info (str "Unexpected error creating new ledger: " (ex-message e))
                 {:status 500
                  :error  :db/unexpected-error}
                 e)))))

(defn broadcast!
  "Responsible for producing the event broadcast to connected peers."
  [{:keys [fluree/watcher fluree/subscriptions] :as _config} commit-result]
  (broadcast/broadcast-new-commit! subscriptions watcher {} commit-result))
