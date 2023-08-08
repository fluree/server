(ns fluree.server.consensus.handlers.ledger-created
  (:require [clojure.core.async :as async]
            [clojure.java.io :as io]
            [fluree.db.conn.file :as file-conn]
            [fluree.db.conn.proto :as conn-proto]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.log :as log]
            [fluree.server.components.watcher :as watcher])
  (:import (java.io File)))

(set! *warn-on-reflection* true)

(defn verify-still-pending
  "Just received a new ledger creation commit, do a check to ensure the current raft
  state still has this request pending."
  [{:keys [consensus/state-atom] :as _config}
   {:keys [ledger-id] :as create-result}]
  (when-not (get-in @state-atom [:create-ledger ledger-id])
    (throw (ex-info (str "Cannot accept new ledger creation for ledger: " ledger-id
                         " as it is no longer queued for creation.")
                    {:status 500 :error :consensus/unexpected-error})))
  create-result)


(defn file-path
  "Returns canonical path to write a commit/data file to disk but
  only if the conn type is of type 'file' - else assumes it is using
  networked storage (e.g. S3, IPFS) and does not need to write out file."
  [{:keys [fluree/conn] :as _config} address]
  (let [[_ conn-type path] (re-find #"^fluree:([^:]+)://(.+)" address)]
    (log/debug "Consensus write file with address: " address " of conn type: " conn-type)
    (when (= "file" conn-type)
      (str (file-conn/local-path conn) "/" path))))


(defn write-file
  "Only writes file to disk if address is of type 'file'

  See fluree.db.conn.file namespace for key/vals contained in `file-meta` map."
  [config {:keys [address json] :as _file-meta}]
  (when-let [file-path (file-path config address)]
    (file-conn/write-file file-path (.getBytes ^String json))))


(defn store-ledger-files
  "Persist both the data-file and commit-file to disk only if redundant
  local-storage. If using a networked file system (e.g. S3, IPFS) the
  file is already stored by the leader with the respective service."
  [{:keys [consensus/raft-state fluree/conn] :as config}
   {:keys [data-file-meta commit-file-meta context-file-meta server ledger-id] :as create-result}]
  (go-try
    (let [this-server (:this-server raft-state)
          address-ch  (conn-proto/-address conn ledger-id nil)] ;; launch address lookup in background, don't block
      (when (not= server this-server)
        ;; if server that created the new ledger is this server, the files
        ;; were already written - only other servers need to write file
        (write-file config data-file-meta)
        (write-file config commit-file-meta)
        (when context-file-meta
          (write-file config context-file-meta))
        (<? (conn-proto/-push conn (<? address-ch) commit-file-meta)))
      create-result)))


(defn add-ledger-to-state
  [{:keys [consensus/state-atom] :as _config}
   {:keys [ledger-id commit-file-meta t] :as create-result}]
  (try
    (swap! state-atom
           (fn [current-state]
             (-> current-state
                 ;; remove the :create-ledger queued request
                 (update :create-ledger dissoc ledger-id)
                 ;; add in the new ledger to state
                 (assoc-in [:ledgers ledger-id] {:t              t
                                                 :status         :ready
                                                 :commit-address (:address commit-file-meta)}))))
    ;; return original params for broadcast step
    create-result

    (catch Exception e
      ; return exception (don't throw) for handler on error
      (log/warn (ex-message e))
      (if (ex-data e)
        e
        (ex-info (str "Unexpected error queuing new ledger: " (ex-message e))
                 {:status 500
                  :error  :db/unexpected-error}
                 e)))))


(defn return-success-response
  [watcher {:keys [ledger-id server tx-id] :as params} state-map]
  (log/info (str "New Ledger successfully created by server " server ": " ledger-id " with tx-id: " tx-id "."))
  (watcher/deliver-watch watcher tx-id params)
  (get-in state-map [:ledgers ledger-id]))


(defn clean-up-files
  [{:keys [fluree/conn] :as _config} {:keys [ledger-id] :as _params}]
  (let [local-path (file-conn/local-path conn)
        ledger-dir (io/file local-path ledger-id)
        files      (reverse (file-seq ledger-dir))]
    (doseq [^File file files]
      (log/info (str "After exception creating ledger " ledger-id ", removing: " (.getPath file)))
      (io/delete-file file true))))


(defn handler
  "Adds a new ledger along with its transaction into the state machine in a queue.
  The consensus leader is responsible for creating all new ledgers, and will get
  to it ASAP."
  [config create-result]
  (log/debug (str "New ledger created with params: " create-result))
  (try

    (->> create-result
         (verify-still-pending config)
         (store-ledger-files config)
         async/<!!
         (add-ledger-to-state config))

    (catch Exception e
      (log/warn (str "Unexpected error creating new ledger: " (ex-message e)))
      (clean-up-files config create-result)
      (let [e* (if (ex-data e)
                 e
                 (ex-info (str "Unexpected error creating new ledger: " (ex-message e))
                          {:status 500
                           :error  :db/unexpected-error}
                          e))]

        e*))))

(defn broadcast!
  "Responsible for producing the event broadcast to connected peers."
  [{:keys [fluree/watcher] :as _config} {:keys [ledger-id server tx-id] :as handler-result}]
  (log/info (str "New Ledger successfully created by server " server ": " ledger-id " with tx-id: " tx-id "."))
  (watcher/deliver-watch watcher tx-id handler-result)
  :success) ;; result of this function is not used