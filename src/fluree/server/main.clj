(ns fluree.server.main
    (:require [fluree.http-api.system :as fha]))

(set! *warn-on-reflection* true)

(defn wrap-log-request
      [handler]
      (fn [req]
          (println "Nexus req keys:" (-> req keys pr-str))
          (println "Nexus request:" (pr-str (select-keys req [:parameters :headers])))
          (handler req)))

(defn wrap-log-response
      [handler]
      (fn [req]
          (let [resp (handler req)]
               (println "Nexus response:" (pr-str resp))
               resp)))

(defn wrap-record-fuel-usage
      [handler]
      (fn [req]
          (let [resp (handler req)
                fuel (or (some-> resp
                                 (get-in [:headers "x-fdb-fuel"])
                                 Integer/parseInt)
                         500)]
               (println "Nexus fuel usage:" fuel)
               resp)))

(def ledger-defaults
  {:context {:id "@id", :type "@type"
             :ex "http://example.com/"}})

(defn -main
      [& args]
      (fha/run-server {:fluree/connection
                                    {:method   :file
                                     :defaults ledger-defaults}
                       :http/server {:middleware [[0 wrap-log-request]
                                                  [0 wrap-log-response]
                                                  [90 wrap-record-fuel-usage]]}}))
