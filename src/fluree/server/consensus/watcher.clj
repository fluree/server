(ns fluree.server.consensus.watcher
  "System to manage and track pending requests through consensus in order to
  respond with the result. When mutation requests happen over http (e.g.
  creating a new ledger), we want to wait for the operation to complete through
  consensus before providing the response back to the API requester. These
  operations get delivered via consensus, where they end up being picked up by a
  responsible server from a queue, processed, and then the results broadcast
  back out."
  (:refer-clojure :exclude [remove-watch])
  (:require [clojure.core.async :as async]
            [fluree.db.util.log :as log]))

(defn new-watcher-atom
  "This atom maps a request's unique id (e.g. tx-id) to a promise that will be
  delivered"
  []
  (atom {}))

(defn remove-watch
  [watcher id]
  (swap! (:watcher-atom watcher) dissoc id))

(defn create-watch
  "Creates a new watch for transaction `id`.
  If response not delivered by max-tx-wait-ms, will deliver :timeout response.

  Note: purposefully do not close! timeout chan when delivering a response,
  core async shares similar timeout chans (< 10 ms difference) and closing
  can result in closing a different watch's timeout inadvertently."
  [{:keys [watcher-atom max-tx-wait-ms] :as watcher} id]
  (let [promise-ch (async/promise-chan)]
    (swap! watcher-atom assoc id promise-ch)
    (async/go
      (async/<! (async/timeout max-tx-wait-ms))
      ;; we close the promise chan when delivering, so put! will be false if response already delivered
      (when (async/put! promise-ch :timeout)
        (log/debug "Timed out waiting for transaction to complete:" id
                   "after" max-tx-wait-ms "ms."))
      (remove-watch watcher id))
    ;; return promise ch, will end up with result or closed at end of wait-ms
    promise-ch))

(defn deliver-watch
  [{:keys [watcher-atom] :as watcher} id response]
  ;; note: this can have a race condition, but given it is a promise chan, the second put! will be ignored
  (when-let [resp-chan (get @watcher-atom id)]
    (remove-watch watcher id)
    (async/put! resp-chan response)
    (async/close! resp-chan)))

(defn watch
  [{:keys [max-tx-wait-ms watcher-atom]
    :or   {max-tx-wait-ms 60000}}]
  {:max-tx-wait-ms max-tx-wait-ms
   :watcher-atom   (or watcher-atom
                       (new-watcher-atom))})

(defn close
  [watcher]
  (let [watcher-atom (:watcher-atom watcher)
        watches      (vals @watcher-atom)]
    (run! async/close! watches)
    (reset! watcher-atom {})))
