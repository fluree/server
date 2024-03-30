(ns fluree.server.consensus
  "To allow for pluggable consensus, we have a TxGroup protocol. In order to allow
  for a new consensus type, we need to create a record with all of the following
  methods. Currently, we support a Raft and Solo.")

(set! *warn-on-reflection* true)

(defprotocol TxGroup
  (queue-new-ledger [group ledger-id tx-id txn opts])
  (queue-new-transaction [group ledger-id tx-id txn opts])
  (-new-entry-async [group entry] "Sends a command to the leader. If no callback provided, returns a core async promise channel that will eventually contain a response."))
