(ns fluree.server.consensus.response
  (:require [fluree.server.broadcast :as broadcast]
            [fluree.server.consensus.events :as events]
            [fluree.server.watcher :as watcher]))

(defn announce-new-ledger
  [watcher broadcaster ledger-id tx-id commit-result]
  (let [new-ledger-event (events/ledger-created ledger-id tx-id commit-result)]
    (broadcast/broadcast-new-ledger! broadcaster new-ledger-event)
    (watcher/deliver-event watcher (events/watcher-id new-ledger-event) new-ledger-event)
    ::new-ledger))

(defn announce-dropped-ledger
  [watcher broadcaster ledger-id drop-result]
  (let [dropped-ledger-event (events/ledger-dropped ledger-id drop-result)]
    (broadcast/broadcast-new-ledger! broadcaster dropped-ledger-event)
    (watcher/deliver-event watcher (events/watcher-id dropped-ledger-event) dropped-ledger-event)
    ::dropped-ledger))

(defn announce-commit
  [watcher broadcaster ledger-id tx-id commit-result]
  (let [commit-event (events/transaction-committed ledger-id tx-id commit-result)]
    (broadcast/broadcast-new-commit! broadcaster commit-event)
    (watcher/deliver-event watcher (events/watcher-id commit-event) commit-event)
    ::commit))

(defn announce-error
  [watcher broadcaster ledger-id tx-id ex]
  (let [error-event (events/error ledger-id ex :tx-id tx-id)]
    (broadcast/broadcast-error! broadcaster error-event)
    (watcher/deliver-event watcher (events/watcher-id error-event) error-event)
    ::error))
