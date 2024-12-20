(ns fluree.server.consensus.watcher
  "System to manage and track pending requests through consensus in order to
  respond with the result. When mutation requests happen over http (e.g.
  creating a new ledger), we want to wait for the operation to complete through
  consensus before providing the response back to the API requester. These
  operations get delivered via consensus, where they end up being picked up by a
  responsible server from a queue, processed, and then the results broadcast
  back out."
  (:refer-clojure :exclude [remove-watch])
  (:require [clojure.core.async :as async :refer [<!]]
            [fluree.db.util.log :as log]))

(defprotocol Watcher
  (create-watch [w id])
  (remove-watch [w id])
  (deliver-watch [w id response]))

(defn new-watcher-atom
  "This atom maps a request's unique id (e.g. tx-id) to a promise that will be
  delivered"
  []
  (atom {}))

(defn remove-watch-state
  [watcher-atom id]
  (swap! watcher-atom dissoc id))

(defn create-watch-state
  "Creates a new watch for transaction `id`.
  If response not delivered by max-txn-wait-ms, will deliver :timeout response.

  Note: purposefully do not close! timeout chan when delivering a response,
  core async shares similar timeout chans (< 10 ms difference) and closing
  can result in closing a different watch's timeout inadvertently."
  [state max-txn-wait-ms id]
  (let [promise-ch (async/promise-chan)]
    (swap! state assoc id promise-ch)
    (async/go
      (async/<! (async/timeout max-txn-wait-ms))
      ;; we close the promise chan when delivering, so put! will be false if
      ;; response already delivered
      (when (async/put! promise-ch :timeout)
        (log/debug "Timed out waiting for transaction to complete:" id
                   "after" max-txn-wait-ms "ms."))
      (remove-watch-state state id))
    ;; return promise ch, will end up with result or closed at end of wait-ms
    promise-ch))

(defn deliver-watch-state
  [watcher-atom id response]
  ;; note: this can have a race condition, but given it is a promise chan, the
  ;; second put! will be ignored
  (when-let [resp-chan (get @watcher-atom id)]
    (remove-watch-state watcher-atom id)
    (async/put! resp-chan response)
    (async/close! resp-chan)))

(defrecord LocalWatcher [state max-txn-wait-ms]
  Watcher
  (create-watch [_ id]
    (create-watch-state state max-txn-wait-ms id))
  (remove-watch [_ id]
    (remove-watch-state state id))
  (deliver-watch [_ id response]
    (deliver-watch-state state id response)))

(defn start
  ([max-txn-wait-ms]
   (start max-txn-wait-ms (new-watcher-atom)))
  ([max-txn-wait-ms watcher-atom]
   (->LocalWatcher watcher-atom max-txn-wait-ms)))

(defn stop
  [^LocalWatcher watcher]
  (let [watcher-atom (:state watcher)
        watches      (vals @watcher-atom)]
    (run! async/close! watches)
    (reset! watcher-atom {})))
