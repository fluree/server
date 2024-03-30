(ns fluree.server.consensus
  (:require [clojure.core.async :as async]))

(set! *warn-on-reflection* true)

;; To allow for pluggable consensus, we have a TxGroup protocol.
;; In order to allow for a new consensus type, we need to create a record with all of the following methods.
;; Currently, we support a RaftGroup and SoloGroup.

(defprotocol TxGroup
  (queue-new-ledger [group ledger-id tx-id txn opts])
  (queue-new-transaction [group ledger-id tx-id txn opts])
  (-new-entry-async [group entry] "Sends a command to the leader. If no callback provided, returns a core async promise channel that will eventually contain a response."))

;; STATE MACHINE COMMANDS - for generic state machine

;; get, assoc, dissoc functions

(defn kv-assoc-in-async
  "Writes value to specified key.
  Returns core async channel that will eventually have a response."
  [group ks v]
  (let [command [:assoc-in ks v]]
    (-new-entry-async group command)))

(defn kv-assoc-in
  "Writes value to specified key."
  [group ks v]
  (async/<!! (kv-assoc-in-async group ks v)))
