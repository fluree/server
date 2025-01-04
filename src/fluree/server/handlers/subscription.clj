(ns fluree.server.handlers.subscription
  (:require [clojure.core.async :as async]
            [fluree.server.handlers.shared :refer [defhandler]]
            [fluree.server.consensus.subscriptions :as subscriptions]
            [ring.adapter.jetty9 :as http]))

(set! *warn-on-reflection* true)

(defn websocket-handler
  [conn subscriptions]
  ;; Mostly copy-pasta from
  ;; https://github.com/sunng87/ring-jetty9-adapter/blob/master/examples/rj9a/websocket.clj
  (fn [upgrade-request]
    (let [provided-subprotocols (:websocket-subprotocols upgrade-request)
          provided-extensions   (:websocket-extensions upgrade-request)
          subscription-id       (str (random-uuid))
          subscription-chan     (async/chan)]
      {;; provide websocket callbacks
       :on-connect  (fn on-connect [ws]
                      (subscriptions/client-message
                       {:msg-type             :on-connect
                        :http/ws              ws
                        :http/sub-id          subscription-id
                        :http/sub-chan        subscription-chan
                        :fluree/subscriptions subscriptions
                        :fluree/connection    conn}))
       :on-text     (fn on-text [ws text-message]
                      (subscriptions/client-message
                       {:msg-type             :on-text
                        :payload              text-message
                        :http/ws              ws
                        :http/sub-id          subscription-id
                        :http/sub-chan        subscription-chan
                        :fluree/subscriptions subscriptions
                        :fluree/connection    conn}))
       :on-bytes    (fn on-bytes [ws payload offset len]
                      (subscriptions/client-message
                       {:msg-type             :on-bytes
                        :payload              payload
                        :offset               offset
                        :len                  len
                        :http/ws              ws
                        :http/sub-id          subscription-id
                        :http/sub-chan        subscription-chan
                        :fluree/subscriptions subscriptions
                        :fluree/connection    conn}))
       :on-close    (fn on-close [ws status-code reason]
                      (subscriptions/client-message
                       {:msg-type             :on-close
                        :status-code          status-code
                        :reason               reason
                        :http/ws              ws
                        :http/sub-id          subscription-id
                        :http/sub-chan        subscription-chan
                        :fluree/subscriptions subscriptions
                        :fluree/connection    conn}))
       :on-ping     (fn on-ping [ws payload]
                      (subscriptions/client-message
                       {:msg-type             :on-ping
                        :payload              payload
                        :http/ws              ws
                        :http/sub-id          subscription-id
                        :http/sub-chan        subscription-chan
                        :fluree/subscriptions subscriptions
                        :fluree/connection    conn}))
       :on-error    (fn on-error [ws e]
                      (subscriptions/client-message
                       {:msg-type             :on-error
                        :error                e
                        :http/ws              ws
                        :http/sub-id          subscription-id
                        :http/sub-chan        subscription-chan
                        :fluree/subscriptions subscriptions
                        :fluree/connection    conn}))
       :subprotocol (first provided-subprotocols)
       :extensions  provided-extensions})))

(defhandler default
  [{:fluree/keys [conn subscriptions] :as _req}]
  (http/ws-upgrade-response (websocket-handler conn subscriptions)))
