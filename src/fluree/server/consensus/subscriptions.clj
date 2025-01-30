(ns fluree.server.consensus.subscriptions
  (:require [clojure.core.async :as async]
            [fluree.db.util.json :as json]
            [fluree.db.util.log :as log]
            [fluree.server.consensus.broadcast :refer [Broadcaster]]
            [ring.adapter.jetty9.websocket :as ws])
  (:import (java.io Closeable IOException)
           (java.nio.channels ClosedChannelException)))

(defn close-chan
  [chan message]
  ;; events are of structure [ledger-id event-name event-data]
  (when message
    (async/put! chan [nil :closing message]))
  (async/close! chan))

(defn all-chans
  [sub-state]
  (->> sub-state
       vals
       (map :chan)))

(defn all-sub-ids
  [sub-state]
  (keys sub-state))

(defn all-sockets
  [sub-state]
  (->> sub-state
       vals
       (map :ws)))

(defn close-all-chans
  "When shutting down, sends a closing event to all channels immediately
  before closing connection."
  [sub-state]
  (->> (all-chans sub-state)
       (run! (fn [chan]
               (close-chan chan "Shutting down"))))
  sub-state)

;; TODO - send proper closing code
(defn close-all-sockets
  [sub-state]
  (->> (all-sockets sub-state)
       (run! (fn [ws]
               (ws/close! ws))))
  sub-state)

(defn replace-existing
  [{:keys [chan ws] :as sub} sub-chan websocket]
  (when chan
    (close-chan chan "Duplicate subscription"))
  (when ws
    (ws/close! ws))
  (assoc sub :chan sub-chan, :ws websocket))

(defn establish-subscription-state
  [state sub-id sub-chan websocket]
  (swap! state update sub-id replace-existing sub-chan websocket))

(defn close-subscription-state
  [state sub-id]
  (let [{:keys [chan ws]} (get @state sub-id)]
    (close-chan chan nil)
    (try (ws/close! ws)
         (catch Exception _ :ignore))
    (swap! state dissoc sub-id)))

(defn send-message
  "Sends a message to an individual socket. If an exception occurs,
  closes the socket and removes it from the subscription map."
  [sub-state sub-id message]
  (let [ws (get-in @sub-state [sub-id :ws])]
    (try
      (ws/send! ws message)
      (catch IOException _
        (log/info "Websocket channel closed (java.io.IOException) for sub-id:" sub-id))
      (catch ClosedChannelException _
        (log/info "Websocket channel closed for sub-id: " sub-id)
        (close-subscription-state sub-state sub-id))
      (catch Exception e
        (log/error e "Error sending message to websocket subscriber:" sub-id)
        (close-subscription-state sub-state sub-id)))))

(defn send-message-to-all
  "Sends a message to all subscriptions. Message sent as JSON stringified map
  in the form: {action: commit, ledger: my/ledger, data: {...}}"
  [sub-state action ledger-alias data]
  (let [data*   (if (string? data)
                  (json/parse data false)
                  data)
        message (json/stringify {"action" action
                                 "ledger" ledger-alias
                                 "data"   data*})
        sub-ids (all-sub-ids @sub-state)]
    (run! #(send-message sub-state % message) sub-ids)))

(defrecord Subscriptions [state]
  Broadcaster
  (broadcast-new-ledger [_ ledger-id commit-addr]
    (send-message-to-all state "ledger-created" ledger-id {:commit commit-addr, :t 1}))
  (broadcast-commit [_ ledger-id t commit-addr]
    (send-message-to-all state "new-commit" ledger-id {:commit commit-addr, :t t}))

  Closeable
  (close [_]
    (log/info "Closing all subscriptions.")
    (-> @state close-all-chans close-all-sockets)))

(def new-state
  {})

(defn listen
  []
  (-> new-state atom ->Subscriptions))

(defn close
  [^Subscriptions subs]
  (.close subs))

(defn establish-subscription
  "Establish a new communication subscription with a client using async chan
  to send messages"
  [{:keys [state] :as _subscriptions} websocket sub-id sub-chan opts]
  (log/debug "Establishing new subscription id:" sub-id "with opts" opts)
  (establish-subscription-state state sub-id sub-chan websocket))

(defn close-subscription
  [{:keys [state] :as _subscriptions} sub-id status-code reason]
  (log/debug "Closing websocket subscription for sub-id:" sub-id
             "with status code:" status-code "and reason:" reason)
  (close-subscription-state state sub-id))

(defn subscribe-ledger
  "Subscribe to ledger"
  [{:keys [state] :as _subscriptions} sub-id {:keys [ledger serialization] :as request}]
  (log/debug "Subscribe websocket request by sub-id" sub-id "request:" request)
  (swap! state update sub-id
         (fn [existing-sub]
           (if existing-sub
             (assoc-in existing-sub [:ledgers ledger] {:serialization serialization})
             (log/info "Existing subscription for sub-id: " sub-id "cannot be found, assume just closed.")))))

(defn unsubscribe-ledger
  "Unsubscribe from ledger"
  [{:keys [state] :as _subscriptions} sub-id {:keys [ledger] :as request}]
  (log/debug "Unsubscribe websocket request by sub-id:" sub-id "request:" request)
  (swap! state update-in [sub-id :ledgers]
         (fn [ledger-subs]
           (if ledger-subs
             (dissoc ledger-subs ledger)
             (log/info "Existing subscription for sub-id:" sub-id "cannot be found, assume just closed.")))))
