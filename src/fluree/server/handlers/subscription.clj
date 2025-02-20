(ns fluree.server.handlers.subscription
  (:require [clojure.core.async :as async]
            [fluree.db.util.json :as json]
            [fluree.db.util.log :as log]
            [fluree.server.broadcast.subscriptions :as subscriptions]
            [fluree.server.handlers.shared :refer [defhandler]]
            [ring.adapter.jetty9 :as http]))

(set! *warn-on-reflection* true)

(defmulti client-message
  (fn [msg-type _msg-ctx]
    msg-type))

(defmethod client-message :connect
  [_ {:keys [fluree/subscriptions http/sub-id http/sub-chan http/ws]}]
  (subscriptions/establish-subscription subscriptions ws sub-id sub-chan nil))

(defmethod client-message :text
  [_ {:keys [fluree/subscriptions http/sub-id payload]}]
  (try
    (let [request (json/parse payload true)
          {:keys [action]} request]
      (case action
        "subscribe"
        (subscriptions/subscribe-ledger subscriptions sub-id request)

        "unsubscribe"
        (subscriptions/unsubscribe-ledger subscriptions sub-id request)

        "ping" ;; Note in-browser websocket doesn't support ping/pong, so this is a no-op to keep alive
        (do
          (log/trace "Websocket keep-alive from sub-id:" sub-id)
          nil)))
    (catch Exception e
      (log/error e "Error with :text message from websocket subscriber:" sub-id))))

(defmethod client-message :bytes
  [_ {:keys [http/sub-id]}]
  (log/info "websocket :bytes (no-op) message for sub-id:" sub-id))

;; pong handled automatically, no response needed
(defmethod client-message :ping
  [_ {:keys [http/sub-id payload]}]
  (log/trace "ws :ping message received from:" sub-id "with payload:" payload))

(defmethod client-message :error
  [_ {:keys [http/sub-id error]}]
  (log/warn "Websocket error for sub-id:" sub-id "with error:" (type error)))

(defmethod client-message :close
  [_ {:keys [fluree/subscriptions http/sub-id status-code reason]}]
  (subscriptions/close-subscription subscriptions sub-id status-code reason))

(defn websocket-handler
  [conn subscriptions]
  ;; Mostly copy-pasta from
  ;; https://github.com/sunng87/ring-jetty9-adapter/blob/master/examples/rj9a/websocket.clj
  (fn [upgrade-request]
    (let [provided-subprotocols (:websocket-subprotocols upgrade-request)
          provided-extensions   (:websocket-extensions upgrade-request)
          subscription-id       (str (random-uuid))
          subscription-chan     (async/chan)
          socket-ctx            {:http/sub-id          subscription-id
                                 :http/sub-chan        subscription-chan
                                 :fluree/subscriptions subscriptions
                                 :fluree/connection    conn}]
      ;; provide websocket callback map
      {:on-connect  (fn on-connect [ws]
                      (client-message :connect (assoc socket-ctx :http/ws ws)))
       :on-text     (fn on-text [ws text-message]
                      (client-message :text (assoc socket-ctx
                                                   :http/ws ws
                                                   :payload text-message)))
       :on-bytes    (fn on-bytes [ws payload offset length]
                      (client-message :bytes (assoc socket-ctx
                                                    :http/ws ws
                                                    :payload payload
                                                    :offset offset
                                                    :length length)))
       :on-close    (fn on-close [ws status-code reason]
                      (client-message :close (assoc socket-ctx
                                                    :http/ws ws
                                                    :status-code status-code
                                                    :reason reason)))
       :on-ping     (fn on-ping [ws payload]
                      (client-message :ping (assoc socket-ctx
                                                   :http/ws ws
                                                   :payload payload)))
       :on-error    (fn on-error [ws e]
                      (client-message :error (assoc socket-ctx
                                                    :http/ws ws
                                                    :error e)))
       :subprotocol (first provided-subprotocols)
       :extensions  provided-extensions})))

(defhandler default
  [{:fluree/keys [conn subscriptions] :as _req}]
  (http/ws-upgrade-response (websocket-handler conn subscriptions)))
