(ns fluree.server.consensus.watcher
  "System to manage and track pending requests through consensus in order to
  respond with the result. When mutation requests happen over http (e.g.
  creating a new ledger), we want to wait for the operation to complete through
  consensus before providing the response back to the API requester. These
  operations get delivered via consensus, where they end up being picked up by a
  responsible server from a queue, processed, and then the results broadcast
  back out."
  (:require [clojure.core.async :as async]
            [fluree.db.util.log :as log]))

(set! *warn-on-reflection* true)

(defprotocol Watcher
  (create-watch [w id])
  (-deliver-commit [w id t ledger-id commit-address])
  (-deliver-error [w id error]))

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
    (when (and max-txn-wait-ms
               (< max-txn-wait-ms 0))
      (async/go
        (async/<! (async/timeout max-txn-wait-ms))
        ;; we close the promise chan when delivering, so put! will be false if
        ;; response already delivered
        (when (async/put! promise-ch :timeout)
          (log/debug "Timed out waiting for transaction to complete:" id
                     "after" max-txn-wait-ms "ms."))
        (remove-watch-state state id)))
    ;; return promise ch, will end up with result or closed at end of wait-ms
    promise-ch))

(defn deliver-commit-state
  [watcher-atom id ledger-id t commit-address]
  ;; note: this can have a race condition, but given it is a promise chan, the
  ;; second put! will be ignored
  (when-let [resp-chan (get @watcher-atom id)]
    (let [response {:tx-id          id
                    :ledger-id      ledger-id
                    :t              t
                    :commit commit-address}]
      (remove-watch-state watcher-atom id)
      (async/put! resp-chan response)
      (async/close! resp-chan))))

(defn deliver-error-state
  [watcher-atom id error]
  ;; note: this can have a race condition, but given it is a promise chan, the
  ;; second put! will be ignored
  (when-let [resp-chan (get @watcher-atom id)]
    (remove-watch-state watcher-atom id)
    (async/put! resp-chan error)
    (async/close! resp-chan)))

(defrecord TimeoutWatcher [state max-txn-wait-ms]
  Watcher
  (create-watch [_ id]
    (create-watch-state state max-txn-wait-ms id))
  (-deliver-commit [_ id t ledger-id commit-address]
    (deliver-commit-state state id t ledger-id commit-address))
  (-deliver-error [_ id error]
    (deliver-error-state state id error)))

(defn deliver-commit
  [w id event]
  (let [{:keys [ledger-id t commit]} event]
    (-deliver-commit w id ledger-id t commit)))

(defn deliver-error
  [w id error]
  (-deliver-error w id error))

(defn start
  ([]
   (start 0))
  ([max-txn-wait-ms]
   (->TimeoutWatcher (new-watcher-atom) max-txn-wait-ms)))

(defn stop
  [^TimeoutWatcher watcher]
  (let [watcher-atom (:state watcher)
        watches      (vals @watcher-atom)]
    (run! async/close! watches)
    (reset! watcher-atom {})))
