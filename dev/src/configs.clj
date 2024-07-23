(ns configs
  (:require [clojure.java.io :as io]
            [aero.core :as aero]
            [clojure.string :as str]
            [meta-merge.core :refer [meta-merge]]
            [integrant.core :as ig]))

(defn load-config
  "Loads aero config at given file path using profile keyword."
  [config-file profile]
  (-> config-file
      io/resource
      (aero/read-config {:profile profile})))

(defn dev-config
  []
  (load-config "config.edn" :dev))

;; raft configs

(def server-1 "/ip4/127.0.0.1/tcp/62071")
(def server-2 "/ip4/127.0.0.1/tcp/62072")
(def server-3 "/ip4/127.0.0.1/tcp/62073")
(def servers-str (str/join "," [server-1 server-2 server-3]))

(def raft-single-server
  (-> (dev-config)
      (dissoc :fluree/standalone)
      (assoc :fluree/raft
             {
              :storage-type     :file,
              :ledger-directory "dev/data-raft"
              :log-directory    "dev/data-raft"
              :this-server      server-1
              :servers          server-1
              :catch-up-rounds  10
              :entries-max      50,
              :log-history      10,
              :conn             (ig/ref :fluree/connection)
              :watcher          (ig/ref :fluree/watcher)
              :subscriptions    (ig/ref :fluree/subscriptions)})))

(def raft-three-server-1
  (meta-merge raft-single-server
              {:http/jetty  {:port 58090}
               :fluree/raft {:ledger-directory "dev/data-raft1"
                             :log-directory    "dev/data-raft1"
                             :this-server      server-1
                             :servers          servers-str}}))

(def raft-three-server-2
  (meta-merge raft-single-server
              {:http/jetty  {:port 58091}
               :fluree/raft {:ledger-directory "dev/data-raft2"
                             :log-directory    "dev/data-raft2"
                             :this-server      server-2
                             :servers          servers-str}}))

(def raft-three-server-3
  (meta-merge raft-single-server
              {:http/jetty  {:port 58092}
               :fluree/raft {:ledger-directory "dev/data-raft3"
                             :log-directory    "dev/data-raft3"
                             :this-server      server-3
                             :servers          servers-str}}))
