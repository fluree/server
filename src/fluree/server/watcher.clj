(ns fluree.server.watcher
  "System to manage and track pending requests through consensus in order to
  respond with the result. When mutation requests happen over http (e.g.
  creating a new ledger), we want to wait for the operation to complete through
  consensus before providing the response back to the API requester. These
  operations get delivered via consensus, where they end up being picked up by a
  responsible server from a queue, processed, and then the results broadcast
  back out."
  (:require [clojure.core.async :as async :refer [<! go]]
            [fluree.db.util.log :as log]))

(set! *warn-on-reflection* true)

(defprotocol Watcher
  (create-watch [w id])
  (watching? [w id])
  (deliver-event [w id event]))

(defn new-watcher-atom
  "This atom maps a request's unique id (e.g. tx-id) to a promise that will be
  delivered"
  []
  (atom {}))

(defn create-watch-state
  "Creates a new watch for transaction `id`.
  If response not delivered by max-txn-wait-ms, will deliver :timeout response.

  Note: purposefully do not close! timeout chan when delivering a response,
  core async shares similar timeout chans (< 10 ms difference) and closing
  can result in closing a different watch's timeout inadvertently."
  [state max-txn-wait-ms id]
  (let [promise-ch (async/promise-chan)]
    (swap! state assoc id promise-ch)
    (when max-txn-wait-ms
      (go
        (<! (async/timeout max-txn-wait-ms))
        ;; we close the promise chan when delivering, so put! will be false if
        ;; response already delivered
        (when (async/put! promise-ch :timeout)
          (log/debug "Timed out waiting for transaction to complete:" id
                     "after" max-txn-wait-ms "ms."))
        (swap! state dissoc id)))
    ;; return promise ch, will end up with result or closed at end of wait-ms
    promise-ch))

(defn deliver-event-state
  [current-state id event]
  (if-let [resp-chan (get current-state id)]
    (do (doto resp-chan
          (async/put! event)
          async/close!)
        (dissoc current-state id))
    current-state))

(defrecord TimeoutWatcher [state max-txn-wait-ms]
  Watcher
  (create-watch [_ id]
    (create-watch-state state max-txn-wait-ms id))
  (watching? [_ id]
    (contains? @state id))
  (deliver-event [_ id event]
    ;; note: this can have a race condition, but given it is a promise chan, the
    ;; second put! will be ignored
    (swap! state deliver-event-state id event)))

(defn start
  ([]
   (start nil))
  ([max-txn-wait-ms]
   (->TimeoutWatcher (new-watcher-atom) max-txn-wait-ms)))

(defn stop
  [^TimeoutWatcher watcher]
  (let [watcher-atom (:state watcher)
        watches      (vals @watcher-atom)]
    (run! async/close! watches)
    (reset! watcher-atom {})))
