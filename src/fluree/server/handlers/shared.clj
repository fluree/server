(ns fluree.server.handlers.shared
  (:require [clojure.core.async :as async :refer [go <!]]
            [fluree.db.util.core :as util]
            [fluree.db.util.json :as json]
            [fluree.db.util.log :as log]
            [fluree.server.consensus.events :as events]
            [fluree.server.watcher :as watcher])
  (:import (clojure.lang ExceptionInfo)
           (java.util Base64)))

(set! *warn-on-reflection* true)

(defn deref!
  "Derefs promise p and throws if the result is an exception, returns it otherwise."
  [p]
  (let [res @p]
    (if (util/exception? res)
      (throw res)
      res)))

(defmacro defhandler
  [name args & body]
  `(defn ~name ~args
     (try
       ~@body
       (catch ExceptionInfo e#
         (if (-> e# ex-data (contains? :response))
           (throw e#)
           (let [{status# :status
                  :as     data#
                  :or     {status# 500}} (ex-data e#)

                 msg#   (ex-message e#)
                 error# (dissoc data# :status)]
             (throw (ex-info "Error in ledger handler"
                             {:response
                              {:status status#
                               :body   (assoc error# :message msg#)}}
                             e#)))))
       (catch Throwable t#
         (throw (ex-info "Error in ledger handler"
                         {:response {:status 500
                                     :body   {:error (ex-message t#)}}}
                         t#))))))

(defn with-header
  [response header value]
  (update response :headers assoc header value))

(defn with-time-header
  [response time]
  (with-header response "x-fdb-time" time))

(defn with-fuel-header
  [response fuel]
  (with-header response "x-fdb-fuel" (str fuel)))

(defn base64-encode
  [^String s]
  (.encodeToString (Base64/getEncoder) (.getBytes s)))

(defn with-policy-header
  [response policy]
  (let [encoded-policy (-> policy json/stringify base64-encode)]
    (with-header response "x-fdb-policy" encoded-policy)))

(defn with-tracking-headers
  [response {:keys [time fuel policy]}]
  (cond-> response
    time   (with-time-header time)
    fuel   (with-fuel-header fuel)
    policy (with-policy-header policy)))

(defn monitor-consensus-persistence
  [watcher ledger-id resp-chan & {:keys [tx-id]}]
  (go
    (let [resp (<! resp-chan)]
      ;; check for exception from trying to put event in consensus, if so we must
      ;; deliver the watch here, but if successful the consensus process will
      ;; deliver the watch downstream
      (when (util/exception? resp)
        (log/warn resp "Error submitting event.")
        (let [error-event (events/error ledger-id resp :tx-id tx-id)]
          (watcher/deliver-event watcher (or tx-id ledger-id) error-event))))))
