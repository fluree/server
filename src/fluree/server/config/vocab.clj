(ns fluree.server.config.vocab
  (:require [fluree.db.connection.vocab :as conn-vocab]))

(set! *warn-on-reflection* true)

(def api-type
  (conn-vocab/system-iri "API"))

(def consensus-type
  (conn-vocab/system-iri "Consensus"))

(def broadcaster-type
  (conn-vocab/system-iri "Broadcaster"))

(def broadcaster
  (conn-vocab/system-iri "broadcaster"))

(def consensus-protocol
  (conn-vocab/system-iri "consensusProtocol"))

(def http-port
  (conn-vocab/system-iri "httpPort"))

(def max-txn-wait-ms
  (conn-vocab/system-iri "maxTxnWaitMs"))

(def closed-mode
  (conn-vocab/system-iri "closedMode"))

(def root-identities
  (conn-vocab/system-iri "rootIdentities"))

(def raft-servers
  (conn-vocab/system-iri "raftServers"))

(def ledger-directory
  (conn-vocab/system-iri "ledgerDirectory"))

(def log-directory
  (conn-vocab/system-iri "logDirectory"))

(def log-history
  (conn-vocab/system-iri "logHistory"))

(def entries-max
  (conn-vocab/system-iri "entriesMax"))

(def catch-up-rounds
  (conn-vocab/system-iri "catchUpRounds"))

(def this-server
  (conn-vocab/system-iri "thisServer"))

(def max-pending-txns
  (conn-vocab/system-iri "maxPendingTxns"))

(def cors-origins
  (conn-vocab/system-iri "corsOrigins"))
