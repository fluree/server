(ns user
  (:require [clojure.string :as str]
            [fluree.server.handlers.transact :as tx-handler]
            [fluree.server.handlers.create :as create-handler]
            [fluree.server.consensus.raft.handler :as consensus-handler]
            [fluree.server.consensus.raft]
            [fluree.server.system :as system]
            [fluree.db.util.log :as log]
            [configs :as configs]
            [integrant.core :as ig]
            [integrant.repl :refer [clear go halt init reset reset-all]]))

;; Register dev-config as the default config
(def sys-config (ig/expand (configs/dev-config)))

(defn set-config!
  "Sets a new config for use with (go)"
  [config]
  (alter-var-root #'sys-config (constantly config)))

(integrant.repl/set-prep! (fn [] sys-config))

(defn start!
  "Starts dev repl. Optionally provide a config
  to start with."
  ([]
   (go))
  ([config]
   (set-config! (ig/expand config))
   (go)))

(comment

 (start!) ;; default :dev profile config

 (start! configs/raft-single-server)


 )