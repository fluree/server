(ns user
  (:require [clojure.string :as str]
            [fluree.server.handlers.transact :as tx-handler]
            [fluree.server.handlers.create :as create-handler]
            [fluree.server.consensus.raft.handler :as consensus-handler]
            [fluree.server.consensus.raft]
            [clojure.java.io :as io]
            [fluree.server.system :as system]
            [fluree.db.util.log :as log]
            [aero.core :as aero]
            [meta-merge.core :refer [meta-merge]]
            [integrant.core :as ig]
            [integrant.repl :refer [clear go halt init reset reset-all]]))

(defn load-config
  "Loads aero config at given file path using profile keyword."
  [config-file profile]
  (-> config-file
      io/resource
      (aero/read-config {:profile profile})))

(defn dev-config
  []
  (ig/expand
   (load-config "config.edn" :dev)))

;; Register dev-config as the default config
(def sys-config (dev-config))

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

 (start!)


 )