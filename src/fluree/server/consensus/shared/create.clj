(ns fluree.server.consensus.shared.create
  "Shared functions for ledger creation across consensus mechanisms."
  (:require [clojure.string :as str]
            [fluree.db.api :as fluree]
            [fluree.db.util.log :as log]))

(def commit-id-prefix "fluree:commit:sha256:b")

(defn commit-id?
  "Checks if a string is a valid commit ID.
  Commit IDs have format: 'fluree:commit:sha256:b<base32-hash>'"
  [commit-id]
  (and (string? commit-id)
       (str/starts-with? commit-id commit-id-prefix)))

(defn hash-from-id
  "Extracts the hash portion from a commit ID.
  Commit IDs have format: 'fluree:commit:sha256:b<base32-hash>'"
  [commit-id]
  (when (commit-id? commit-id)
    (subs commit-id (count commit-id-prefix))))

(defn genesis-result
  "Creates a commit result map for a genesis commit."
  [ledger]
  (let [db     (fluree/db ledger)
        commit (:commit db)]
    {:db      db
     :address (:address commit)
     :hash    (or (:hash commit)
                  (hash-from-id (:id commit)))}))

(defn file-result
  "Creates a commit result map with file metadata structure.
  Used by Raft consensus. Genesis commits have data files but with empty assertions."
  [ledger]
  (let [db         (fluree/db ledger)
        commit     (:commit db)
        ;; Genesis commits have a data file referenced in the commit
        data-addr  (-> commit :data :address)]
    (when-not data-addr
      (log/warn "Genesis commit missing expected data file address"))
    {:db          db
     :data-file   (when data-addr {:address data-addr})  ; Genesis commits do have data files
     :commit-file {:address (:address commit)
                   :json    nil}  ; Will be populated by consensus
     ;; These fields are expected by the event system
     :address     (:address commit)
     :hash        (or (:hash commit)
                      (hash-from-id (:id commit)))}))