(ns user
  (:require [clojure.core.async :as async]
            [clojure.string :as str]
            [fluree.db.api :as fluree]
            [fluree.db.connection.system :as conn-system]
            [fluree.server.handlers.transact :as tx-handler]
            [fluree.server.handlers.create :as create-handler]
            [fluree.server.consensus.raft.handler :as consensus-handler]
            [fluree.server.consensus.raft]
            [fluree.server.system :as system]
            [fluree.db.util.log :as log]
            [fluree.server.config :as config]
            [integrant.core :as ig]
            [integrant.repl :refer [clear go halt init reset reset-all]]))


;; Register dev-config as the default config
(def sys-config (config/read-resource "file-config.jsonld"))

(defn set-config!
  "Sets a new config for use with (go)"
  [config]
  (alter-var-root #'sys-config (constantly config)))

(integrant.repl/set-prep! (fn []
                            (-> sys-config config/parse conn-system/prepare)))

(defn start!
  "Starts dev repl. Optionally provide a config
  to start with."
  ([]
   (go))
  ([config]
   (set-config! (ig/expand config))
   (go)))
