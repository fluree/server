(ns fluree.server.components.fluree
  (:require [donut.system :as ds]
            [fluree.db.connection :as connection]
            [fluree.db.api :as db]
            [fluree.db.util.log :as log]))

(def conn
  #::ds{:start  (fn [{{:keys [options]} ::ds/config}]
                  (log/debug "Connecting to fluree with options:" options)
                  @(db/connect options))
        :stop   (fn [{::ds/keys [instance]}]
                  ;; TODO: Add a close-connection fn to f.d.json-ld.api
                  (when instance (connection/-close instance)))
        :config {:options (ds/ref [:env :fluree/connection])}})
