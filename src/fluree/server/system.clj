(ns fluree.server.system
  (:require [clojure.string :as str]
            [fluree.db.api :as fluree]
            [fluree.db.json-ld.iri :as iri]
            [fluree.server.config :as config]
            [fluree.server.consensus.raft :as raft]
            [fluree.server.consensus.standalone :as standalone]
            [fluree.server.consensus.subscriptions :as subscriptions]
            [fluree.server.consensus.watcher :as watcher]
            [fluree.server.handler :as handler]
            [fluree.server.task.migrate-sid :as task.migrate-sid]
            [integrant.core :as ig]
            [ring.adapter.jetty9 :as jetty]))

(set! *warn-on-reflection* true)

(derive :fluree/raft :fluree/consensus)
(derive :fluree/standalone :fluree/consensus)

(derive :fluree.storage/file :fluree/content-storage)
(derive :fluree.storage/file :fluree/byte-storage)
(derive :fluree.storage/file :fluree/json-archive)

(derive :fluree.storage/memory :fluree/content-storage)
(derive :fluree.storage/memory :fluree/byte-storage)
(derive :fluree.storage/memory :fluree/json-archive)

(derive :fluree.storage/s3 :fluree/content-storage)
(derive :fluree.storage/s3 :fluree/byte-storage)
(derive :fluree.storage/s3 :fluree/json-archive)

(derive :fluree.storage/ipfs :fluree/content-storage)
(derive :fluree.storage/ipfs :fluree/json-archive)

(derive :fluree.storage/remote-resources :fluree/json-archive)

(derive :fluree.publication/remote-resources :fluree/nameservice)
(derive :fluree.publication/remote-resources :fluree/publication)

(derive :fluree.nameservice/storage-backed :fluree/nameservice)
(derive :fluree.nameservice/storage-backed :fluree/publisher)

(derive :fluree.nameservice/ipns :fluree/nameservice)
(derive :fluree.nameservice/ipns :fluree/publisher)

(derive :fluree.serializer/json :fluree/serializer)

(derive :http/jetty :http/server)

(def system-ns
  "https://ns.flur.ee/system#")

(defn system-iri
  [s]
  (str system-ns s))

(def connection-type
  (system-iri "Connection"))

(def storage-type
  (system-iri "Storage"))

(def publisher-type
  (system-iri "Publisher"))

(def system-type
  (system-iri "System"))

(def consensus-type
  (system-iri "Consensus"))

(def api-type
  (system-iri "API"))

(defn encode-illegal-char
  [c]
  (case c
    "&" "<am>"
    "@" "<at>"
    "]" "<cb>"
    ")" "<cp>"
    ":" "<cl>"
    "," "<cm>"
    "$" "<dl>"
    "." "<do>"
    "%" "<pe>"
    "#" "<po>"
    "(" "<op>"
    "[" "<ob>"
    ";" "<sc>"
    "/" "<sl>"))

(defn kw-encode
  [s]
  (str/replace s #"[:#@$&%.,;~/\(\)\[\]]" encode-illegal-char))

(defn iri->kw
  [iri]
  (->> iri
       iri/decompose
       (map kw-encode)
       (apply keyword)))

(def default-resource-name "config.json")

(defmethod ig/expand-key ::config/connection
  [_ config]
  (let [config* (assoc config :server (ig/ref :fluree/server))]
    {:fluree.server/connection config*}))

(defmethod ig/expand-key ::config/server
  [_ config]
  {:fluree/server config})

(defmethod ig/expand-key ::config/consensus
  [_ config]
  (let [consensus-key    (keyword "fluree" (-> config :protocol name))
        consensus-config (-> config
                             (dissoc :protocol)
                             (assoc :server (ig/ref :fluree/server)
                                    :conn (ig/ref :fluree.server/connection)
                                    :watcher (ig/ref :fluree/watcher)
                                    :subscriptions (ig/ref :fluree/subscriptions)))]
    {consensus-key         consensus-config
     :fluree/subscriptions {}}))

(defmethod ig/expand-key ::config/http
  [_ {:keys [server max-txn-wait-ms] :as config :or {server :jetty}}]
  (let [http-key    (keyword "http" (name server))
        http-config (-> config
                        (dissoc :server :max-txn-wait-ms)
                        (assoc :handler (ig/ref :fluree/handler)))]
    {http-key        http-config
     :fluree/watcher {:max-txn-wait-ms max-txn-wait-ms}
     :fluree/handler {:conn          (ig/ref :fluree.server/connection)
                      :consensus     (ig/ref :fluree/consensus)
                      :watcher       (ig/ref :fluree/watcher)
                      :subscriptions (ig/ref :fluree/subscriptions)}}))

(defmethod ig/expand-key ::config/sid-migration
  [_ config]
  {:fluree/sid-migration (assoc config :conn (ig/ref :fluree.server/connection))})

(defmethod ig/init-key :fluree.server/connection
  [_ {:keys [server] :as config}]
  (let [config* (-> config
                    (assoc :storage-path (:storage-path server))
                    (dissoc :server))]
    @(fluree/connect config*)))

(defmethod ig/init-key :fluree/server
  [_ config]
  config)

(defmethod ig/init-key :fluree/subscriptions
  [_ _]
  (subscriptions/listen))

(defmethod ig/halt-key! :fluree/subscriptions
  [_ subs]
  (subscriptions/close subs))

(defmethod ig/init-key :fluree/watcher
  [_ {:keys [max-txn-wait-ms]}]
  (watcher/watch max-txn-wait-ms))

(defmethod ig/halt-key! :fluree/watcher
  [_ watcher]
  (watcher/close watcher))

(defmethod ig/init-key :fluree/raft
  [_ {:keys [server] :as config}]
  (let [config* (assoc config :ledger-directory (:storage-path server))]
    (raft/start config*)))

(defmethod ig/halt-key! :fluree/raft
  [_ {:keys [close] :as _raft-group}]
  (close))

(defmethod ig/init-key :fluree/standalone
  [_ {:keys [conn subscriptions watcher max-pending-txns]}]
  (standalone/start conn subscriptions watcher max-pending-txns))

(defmethod ig/halt-key! :fluree/standalone
  [_ transactor]
  (standalone/stop transactor))

(defmethod ig/init-key :fluree/handler
  [_ {:keys [conn consensus watcher subscriptions]}]
  (handler/app conn consensus watcher subscriptions))

(defmethod ig/init-key :http/jetty
  [_ {:keys [handler port join?]}]
  (jetty/run-jetty handler {:port port, :join? join?}))

(defmethod ig/halt-key! :http/jetty
  [_ http-server]
  (jetty/stop-server http-server))

(defmethod ig/init-key :fluree/sid-migration
  [_ {:keys [conn ledgers force]}]
  (task.migrate-sid/migrate conn ledgers force))

(defn start-config
  [config]
  (-> config ig/expand ig/init))

(defn start-file
  ([path]
   (start-file path :prod))
  ([path profile]
   (-> path
       (config/load-file profile)
       start-config)))

(defn start-resource
  ([resource-name]
   (start-resource resource-name :prod))
  ([resource-name profile]
   (-> resource-name
       (config/load-resource profile)
       start-config)))

(def start
  (partial start-resource default-resource-name))

(defn stop
  [server]
  (ig/halt! server))
