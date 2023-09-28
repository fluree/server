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
           (let [msg#   (ex-message e#)
                 {status# :status :as data# :or {status# 500}} (ex-data e#)
                 error# (dissoc data# :status)]
             (throw (ex-info "Error in ledger handler"
                             {:response
                              {:status status#
                               :body   (assoc error# :message msg#)}})))))
       (catch Throwable t#
         (throw (ex-info "Error in ledger handler"
                         {:response {:status 500
                                     :body   {:error (ex-message t#)}}}))))))
