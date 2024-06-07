(ns fluree.server.consensus
  "To allow for pluggable consensus, we have a TxGroup protocol. In order to allow
  for a new consensus type, we need to create a record with all of the following
  methods. Currently, we support Raft and Solo."
  (:require [fluree.server.consensus.watcher :as watcher]
            [fluree.db.util.core :as util]
            [clojure.core.async :as async :refer [<! go]]))

(set! *warn-on-reflection* true)

(defprotocol TxGroup
  (-queue-new-ledger [group ledger-id tx-id txn opts])
  (-queue-new-transaction [group ledger-id tx-id txn opts]))

(defn deliver-on-exception
  [watcher tx-id persist-resp-ch]
  (go
    (let [persist-resp (<! persist-resp-ch)]
      ;; check for exception trying to put txn in consensus, if so we must deliver the
      ;; watch here, but if successful the consensus process will deliver the watch downstream
      (when (util/exception? persist-resp)
        (watcher/deliver-watch watcher tx-id persist-resp)))))

(defn queue-new-ledger
  [group watcher ledger tx-id txn opts]
  (let [final-resp-ch   (watcher/create-watch watcher tx-id)
        persist-resp-ch (-queue-new-ledger group ledger tx-id txn opts)]
    (deliver-on-exception watcher tx-id persist-resp-ch)
    final-resp-ch))

(defn queue-new-transaction
  [group watcher ledger tx-id txn opts]
  (let [final-resp-ch   (watcher/create-watch watcher tx-id)
        persist-resp-ch (-queue-new-transaction group ledger tx-id txn opts)]
    (deliver-on-exception watcher tx-id persist-resp-ch)
    final-resp-ch))
