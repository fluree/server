(ns fluree.server.consensus.raft.handlers.new-commit
  (:require [clojure.core.async :as async]
            [fluree.db.nameservice.core :as nameservice]
            [fluree.db.storage :as storage]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.log :as log]
            [fluree.server.components.subscriptions :as subs]
            [fluree.server.components.watcher :as watcher]))

(set! *warn-on-reflection* true)

(defn write-file
  "Only writes file to disk if address is of type 'file'

  See fluree.db.conn.file namespace for key/vals contained in `file-meta` map."
  [{:keys [fluree/conn]} {:keys [address json] :as _file-meta}]
  (let [{:keys [method local]} (storage/parse-address address)]
    (when (= "file" method)
      (async/<!! (storage/write (:store conn) local json)))))

(defn store-ledger-files
  "Persist both the data-file and commit-file to disk only if redundant
  local-storage. If using a networked file system (e.g. S3, IPFS) the
  file is already stored by the leader with the respective service."
  [{:keys [consensus/raft-state fluree/conn] :as config}
   {:keys [data-file-meta commit-file-meta server] :as commit-result}]
  (go-try
    (let [this-server (:this-server raft-state)]
      (when (not= server this-server)
        ;; if server that created the new ledger is this server, the files
        ;; were already written - only other servers need to write file
        (when data-file-meta ;; if the commit is just being updated, there won't be more data (e.g. after indexing)
          (write-file config data-file-meta))
        (write-file config commit-file-meta)
        (<? (nameservice/push! conn commit-file-meta)))
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
  [{:keys [fluree/watcher fluree/subscriptions] :as _config}
   {:keys [ledger-id tx-id server commit-file-meta] :as commit-result}]
  (log/info "New transaction completed for" ledger-id "tx-id: " tx-id "by server:" server)
  (watcher/deliver-watch watcher tx-id commit-result)
  (subs/send-message-to-all subscriptions "new-commit" ledger-id (:json commit-file-meta))
  :success) ;; result of this function is not used
