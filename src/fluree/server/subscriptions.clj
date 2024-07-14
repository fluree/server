(ns fluree.server.subscriptions
  (:require [clojure.core.async :as async]
            [fluree.db.util.json :as json]
            [fluree.db.util.log :as log]
            [ring.adapter.jetty9.websocket :as ws])
  (:import (java.io IOException)
           (java.nio.channels ClosedChannelException)))


;; structure of subscriptions atom:
;{:subs    {"id" {:chan   port ;; <- core.async channel
;                 :ledgers {"some/ledger" {}} ;; <- map where each key is a ledger-id subscribed to
;                 :did     nil}}
; :closed? false}

(defn close-chan
  [chan message]
  ;; events are of structure [ledger-id event-name event-data]
  (when message
    (async/put! chan [nil :closing message]))
  (async/close! chan))

(defn all-chans
  [sub-state]
  (->> sub-state
       :subs
       vals
       (map :chan)))

(defn all-sub-ids
  [sub-state]
  (->> sub-state
       :subs
       keys))

(defn all-sockets
  [sub-state]
  (->> sub-state
       :subs
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

(defn mark-closed
  "Mark all subscriptions as closing to prevent any new ones from registering
  while shutting down."
  [sub-atom]
  (swap! sub-atom assoc :closed? true))

(defn establish-subscription
  "Establish a new communication subscription with a client using async chan
  to send messages"
  [{:keys [sub-atom] :as _subscriptions} websocket sub-id sub-chan opts]
  (log/debug "Establishing new subscription id:" sub-id "with opts" opts)
  (swap! sub-atom update-in [:subs sub-id]
         (fn [{:keys [chan ws] :as existing-sub}]
           (when chan
             (close-chan chan "Duplicate subscription"))
           (when ws
             (ws/close! ws))
           (assoc existing-sub :chan sub-chan :ws websocket))))

(defn close-subscription
  [{:keys [sub-atom] :as subscriptions} sub-id status-code reason]
  (log/debug "Closing websocket subscription for sub-id:" sub-id
             "with status code:" status-code "and reason:" reason)
  (let [{:keys [chan ws]} (get-in @sub-atom [:subs sub-id])]
    (close-chan chan nil)
    (try (ws/close! ws)
         (catch Exception _ :ignore))
    (swap! subscriptions update :subs dissoc sub-id)))

(defn subscribe-ledger
  "Subscribe to ledger"
  [{:keys [sub-atom] :as _subscriptions} sub-id {:keys [ledger serialization] :as request}]
  (log/debug "Subscribe websocket request by sub-id" sub-id "request:" request)
  (swap! sub-atom update-in [:subs sub-id]
         (fn [existing-sub]
           (if existing-sub
             (assoc-in existing-sub [:ledgers ledger] {:serialization serialization})
             (log/info "Existing subscription for sub-id: " sub-id "cannot be found, assume just closed.")))))

(defn unsubscribe-ledger
  "Unsubscribe from ledger"
  [{:keys [sub-atom] :as _subscriptions} sub-id {:keys [ledger] :as request}]
  (log/debug "Unsubscribe websocket request by sub-id:" sub-id "request:" request)
  (swap! sub-atom update-in [:subs sub-id :ledgers]
         (fn [ledger-subs]
           (if ledger-subs
             (dissoc ledger-subs ledger)
             (log/info "Existing subscription for sub-id:" sub-id "cannot be found, assume just closed.")))))

(defn send-message
  "Sends a message to an individual socket. If an exception occurs,
  closes the socket and removes it from the subscription map."
  [{:keys [sub-atom] :as subscriptions} sub-id message]
  (let [ws (get-in @sub-atom [:subs sub-id :ws])]
    (try
      (ws/send! ws message)
      (catch IOException _
        (log/info "Websocket channel closed (java.io.IOException) for sub-id:" sub-id))
      (catch ClosedChannelException _
        (log/info "Websocket channel closed for sub-id: " sub-id)
        (close-subscription subscriptions sub-id nil nil))
      (catch Exception e
        (log/error e "Error sending message to websocket subscriber:" sub-id)
        (close-subscription subscriptions sub-id nil nil)))))

(defn send-ledger-message
  "Sends a message only to subscriptions that subscribed to a ledger.
  Note the message format should be a JSON-stringified map in the form of:
  {action: commit, ledger: myledger, data: {...}}"
  [{:keys [sub-atom] :as subscriptions} ledger-id message]
  (log/debug "Sending ledger message for:" ledger-id "with message:" message)
  (let [{:keys [closed? subs]} @sub-atom]
    (when-not closed?
      (doseq [[sub-id {:keys [ledgers]}] subs]
        (when (contains? ledgers ledger-id)
          ;; TODO - use ledger subscription opts to filter out messages, permission
          (send-message subscriptions sub-id message))))))

(defn send-message-to-all
  "Sends a message to all subscriptions. Message sent as JSON stringified map
  in the form: {action: commit, ledger: my/ledger, data: {...}}"
  [{:keys [sub-atom] :as subscriptions} action ledger-alias data]
  (let [data*   (if (string? data)
                  (json/parse data false)
                  data)
        message (json/stringify {"action" action
                                 "ledger" ledger-alias
                                 "data"   data*})
        sub-ids (all-sub-ids @sub-atom)]
    (run! #(send-message subscriptions % message) sub-ids)))

(defn listen
  []
  {:sub-atom (atom {:subs    {}
                    :closed? false})})

(defn close
  [{:keys [sub-atom] :as _subscriptions}]
  (log/info "Closing all websocket subscriptions.")
  (-> sub-atom
      mark-closed
      close-all-chans
      close-all-sockets)
  (reset! sub-atom {:closed? true}))

(defmulti client-message :msg-type)

(defmethod client-message :on-connect
  [{:keys [fluree/subscriptions http/sub-id http/sub-chan http/ws]}]
  (establish-subscription subscriptions ws sub-id sub-chan nil))

(defmethod client-message :on-text
  [{:keys [fluree/subscriptions http/sub-id payload]}]
  (try
    (let [request (json/parse payload true)
          {:keys [action]} request]
      (case action
        "subscribe"
        (subscribe-ledger subscriptions sub-id request)

        "unsubscribe"
        (unsubscribe-ledger subscriptions sub-id request)

        "ping" ;; Note in-browser websocket doesn't support ping/pong, so this is a no-op to keep alive
        (do
          (log/trace "Websocket keep-alive from sub-id:" sub-id)
          nil)))
    (catch Exception e
      (log/error e "Error with :on-text message from websocket subscriber:" sub-id))))

(defmethod client-message :on-bytes
  [{:keys [http/sub-id _payload _offset _len]}]
  (log/info "websocket :on-bytes (no-op) message for sub-id:" sub-id))

;; pong handled automatically, no response needed
(defmethod client-message :on-ping
  [{:keys [http/sub-id payload]}]
  (log/trace "ws :on-ping message received from:" sub-id "with payload:" payload))

(defmethod client-message :on-error
  [{:keys [http/sub-id error]}]
  (log/warn "Websocket error for sub-id:" sub-id "with error:" (type error)))

(defmethod client-message :on-close
  [{:keys [fluree/subscriptions http/sub-id status-code reason]}]
  (close-subscription subscriptions sub-id status-code reason))
