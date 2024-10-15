(ns fluree.server.config.vocab)

(def system-ns
  "https://ns.flur.ee/system#")

(defn system-iri
  [s]
  (str system-ns s))

(def connection-type
  (system-iri "Connection"))

(def consensus-type
  (system-iri "Consensus"))

(def storage-type
  (system-iri "Storage"))

(def publisher-type
  (system-iri "Publisher"))

(def system-type
  (system-iri "System"))

(def api-type
  (system-iri "API"))

(def address-identifier
  (system-iri "addressIdentifier"))

(def address-identifiers
  (system-iri "addressIdentifiers"))

(def file-path
  (system-iri "filePath"))

(def s3-bucket
  (system-iri "s3Bucket"))

(def s3-prefix
  (system-iri "s3Prefix"))

(def s3-endpoint
  (system-iri "s3Endpoint"))

(def storage
  (system-iri "storage"))

(def ipfs-endpoint
  (system-iri "ipfsEndpoint"))

(def ipns-profile
  (system-iri "ipnsProfile"))

(def consensus-protocol
  (system-iri "consensusProtocol"))

(def http-port
  (system-iri "httpPort"))

(def max-txn-wait-ms
  (system-iri "maxTxnWaitMs"))

(def closed-mode
  (system-iri "closedMode"))

(def root-identities
  (system-iri "rootIdentities"))

(def parallelism
  (system-iri "parallelism"))

(def cache-max-mb
  (system-iri "cachMaxMb"))

(def commit-storage
  (system-iri "commitStorage"))

(def index-storage
  (system-iri "indexStorage"))

(def primary-publisher
  (system-iri "primaryPublisher"))

(def secondary-publishers
  (system-iri "secondaryPublishers"))

(def remote-systems
  (system-iri "remoteSystems"))

(def raft-servers
  (system-iri "raftServers"))

(def server-urls
  (system-iri "serverUrls"))

(def ledger-defaults
  (system-iri "ledgerDefaults"))

(def index-options
  (system-iri "indexOptions"))

(def reindex-min-bytes
  (system-iri "reindexMinBytes"))

(def reindex-max-bytes
  (system-iri "reindexMaxBytes"))

(def max-old-indexes
  (system-iri "maxOldIndexes"))

(def ledger-directory
  (system-iri "ledgerDirectory"))

(def log-directory
  (system-iri "logDirectory"))

(def log-history
  (system-iri "logHistory"))

(def entries-max
  (system-iri "entriesMax"))

(def catch-up-rounds
  (system-iri "catchUpRounds"))

(def this-server
  (system-iri "thisServer"))

(def max-pending-txns
  (system-iri "maxPendingTxns"))

(def connection
  (system-iri "connection"))
