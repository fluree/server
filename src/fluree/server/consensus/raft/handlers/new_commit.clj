(ns fluree.server.consensus.raft.handlers.new-commit
  (:require [clojure.core.async :as async]
            [fluree.db.nameservice.core :as nameservice]
            [fluree.db.storage :as storage]
            [fluree.db.util.async :refer [<? <?? go-try]]
            [fluree.db.util.bytes :as bytes]
            [fluree.db.util.json :as json]
            [fluree.db.util.log :as log]
            [fluree.server.consensus.broadcast :as broadcast]))

(set! *warn-on-reflection* true)

(defn write-file
  "Only writes file to disk if address is of type 'file'

  See fluree.db.conn.file namespace for key/vals contained in `file-meta` map."
  [store {:keys [address json] :as _file-meta}]
  (let [{:keys [method]} (storage/parse-address address)]
    (when (= "file" method)
      (async/<!!
       (storage/write-bytes store address (bytes/string->UTF8 json))))))

(defn push-nameservice
  [{:keys [fluree/conn] :as _config} {:keys [commit-file-meta] :as commit-result}]
  (go-try
   (let [{:keys [json address]} commit-file-meta
         commit-json (-> (json/parse json false)
                         ;; address is not yet written into the commit file, add it
                         (assoc "address" address))]
     (<? (nameservice/push! conn commit-json))
     commit-result)))

(defn store-ledger-files
  "Persist both the data-file and commit-file to disk only if redundant
  local-storage. If using a networked file system (e.g. S3, IPFS) the
  file is already stored by the leader with the respective service."
  [{:keys [consensus/raft-state fluree/store] :as _config}
   {:keys [data-file-meta commit-file-meta] :as commit-result}]
  (go-try
    (let [this-server   (:this-server raft-state)
          commit-server (:server commit-result)]
      (when (not= commit-server this-server)
       ;; if server that created the new ledger is this server, the files
       ;; were already written - only other servers need to write file
        (when data-file-meta ;; if the commit is just being updated, there won't be more data (e.g. after indexing)
          (write-file store data-file-meta))
        (write-file store commit-file-meta))
      commit-result)))

(defn delete-ledger-file
  [store {:keys [address] :as _file-meta}]
  (storage/delete store address))

(defn delete-ledger-files
  "In the case of an exception or the proposed new files do not get accepted by consensus,
  we want to remove the files that were stored."
  [{:keys [fluree/store] :as _config}
   {:keys [data-file-meta commit-file-meta] :as _commit-result}]
  (when data-file-meta ;; if the commit is just being updated, there won't be more data (e.g. after indexing)
    (delete-ledger-file store data-file-meta))
  (delete-ledger-file store commit-file-meta)
  ;; TODO - need to remove the entries from the nameservice
  ::done)

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
                                                 :commit-address (:address commit-file-meta)}))))
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
  (broadcast/announce-new-commit! subscriptions watcher commit-result))
