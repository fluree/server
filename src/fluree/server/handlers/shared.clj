(ns fluree.server.handlers.shared
  (:require [fluree.db.util.core :as util])
  (:import (clojure.lang ExceptionInfo)))

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

(defn with-tracking-headers
  [response {:keys [time fuel]}]
  (cond-> response
    time (with-time-header time)
    fuel (with-fuel-header fuel)))
