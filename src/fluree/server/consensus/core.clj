(ns fluree.server.consensus.core
  (:require [clojure.core.async :as async]
            [clojure.string :as str]
            [fluree.db.conn.proto :as conn-proto]
            [fluree.db.util.log :as log]
            [fluree.raft :as raft]
            [fluree.server.consensus.network.tcp :as ftcp]
            [fluree.server.consensus.raft.core :as raft-helpers]
            [fluree.server.consensus.protocol :as txproto]
            [fluree.server.consensus.network.multi-addr :as multi-addr]
            [fluree.server.io.file :as io-file]))

(set! *warn-on-reflection* true)

(defn this-server
  "Returns current server's name."
  [raft-state]
  (:this-server raft-state))

(defn queue-new-ledger
  "Queues a new ledger into the consensus layer for processing.
  Returns a core async channel that will eventually contain true if successful."
  [group conn-type ledger-id tx-id txn txn-context opts]
  (log/debug "Consensus - queue new ledger:" ledger-id tx-id txn)
  (txproto/-new-entry-async
    group
    [:ledger-create {:txn         txn
                     :txn-context txn-context
                     :conn-type   conn-type
                     :size        (count txn)
                     :tx-id       tx-id
                     :ledger-id   ledger-id
                     :opts        opts
                     :instant     (System/currentTimeMillis)}]))

(defn queue-new-transaction
  "Queues a new transaction into the consensus layer for processing.
  Returns a core async channel that will eventually contain a truthy value if successful."
  [group conn-type ledger-id tx-id txn txn-context opts]
  (log/trace "queue-new-transaction txn:" txn)
  (txproto/-new-entry-async
    group
    [:tx-queue {:txn            txn
                :conn-type      conn-type
                :size           (count txn)
                :tx-id          tx-id
                :ledger-id      ledger-id
                :txn-context    txn-context
                :opts           opts
                :instant        (System/currentTimeMillis)}]))

(defn data-version
  [group]
  (or (:version (raft-helpers/local-state group)) 1))

(defn set-data-version
  [group version]
  (assert (number? version))
  (txproto/kv-assoc-in group [:version] version))

(defn build-snapshot-config
  "Returns a map of the necessary configurations for snapshot reading/writing, etc.
  used automatically by the raft system to handle all snapshot activities automaticallly."
  [{:keys [encryption-key storage-group-read storage-group-write storage-group-exists
           storage-group-delete storage-group-list log-path state-machine-atom] :as _raft-config}]
  {:path           "snapshots/"
   :state-atom     state-machine-atom
   :storage-read   (or storage-group-read
                       (io-file/connection-storage-read
                         log-path
                         encryption-key))
   :storage-write  (or storage-group-write
                       (io-file/connection-storage-write
                         log-path
                         encryption-key))
   :storage-exists (or storage-group-exists
                       (io-file/connection-storage-exists?
                         log-path))
   :storage-delete (or storage-group-delete
                       (io-file/connection-storage-delete
                         log-path))
   :storage-list   (or storage-group-list
                       (io-file/connection-storage-list
                         log-path))})

(defn add-ledger-storage-fns
  "Functions that write and read ledger data. Either supplied with config, or generated
  automatically with defaults"
  [{:keys [encryption-key conn-storage-path
           storage-ledger-read storage-ledger-write] :as raft-config}]
  (assoc raft-config
    :storage-ledger-read (or storage-ledger-read
                             (io-file/connection-storage-read
                               conn-storage-path
                               encryption-key))
    :storage-ledger-write (or storage-ledger-write
                              (io-file/connection-storage-write
                                conn-storage-path
                                encryption-key))))

(defn add-state-machine
  "Add state machine configuration options needed for raft"
  [{:keys [fluree/conn fluree/watcher fluree/subscriptions this-server command-chan storage-ledger-read storage-ledger-write] :as raft-config}
   handler]
  (let [state-machine-atom   (atom raft-helpers/default-state)
        state-machine-config {:fluree/conn                    conn
                              :fluree/watcher                 watcher
                              :fluree/subscriptions           subscriptions
                              :consensus/command-chan         command-chan
                              :consensus/this-server          this-server
                              :consensus/state-atom           state-machine-atom
                              :consensus/ledger-read          storage-ledger-read
                              :consensus/ledger-write         storage-ledger-write
                              :consensus/state-change-fn-atom raft-helpers/state-change-fn-atom}]
    (assoc raft-config :state-machine-atom state-machine-atom
                       :state-machine (raft-helpers/handler handler state-machine-config))))

(defn add-snapshot-config
  [raft-config]
  (let [snapshot-config (build-snapshot-config raft-config)]
    (assoc raft-config :snapshot-write (raft-helpers/snapshot-writer snapshot-config)
                       :snapshot-reify (raft-helpers/snapshot-reify snapshot-config)
                       :snapshot-xfer (raft-helpers/snapshot-xfer snapshot-config)
                       :snapshot-install (raft-helpers/snapshot-installer snapshot-config)
                       :snapshot-list-indexes (raft-helpers/snapshot-list-indexes snapshot-config))))

(defn add-server-configs
  [{:keys [this-server servers] :as raft-state}]
  (when-not (string? servers)
    (throw (ex-info (str "Cannot start raft without a list of participating servers separated by a comma or semicolon. "
                         "If this is a single server, please specify this-server instead of servers.")
                    {:status 400 :error :db/invalid-server-address})))
  (let [servers*           (mapv str/trim (str/split servers #"[,;]"))
        this-server*       (if this-server
                             (str/trim this-server)
                             (if (= 1 (count servers*))
                               (first servers*)
                               (throw (ex-info "Must specify this-server if multiple servers are specified"
                                               {:status 400 :error :db/invalid-server-address}))))
        server-configs-map (mapv multi-addr/multi->map servers*)
        this-server-map    (multi-addr/multi->map this-server*)
        this-server-cfg    (raft-helpers/validated-this-server-config this-server-map server-configs-map)]
    (raft-helpers/validated-raft-servers server-configs-map)
    (assoc raft-state :this-server this-server*
                      :servers servers*
                      :this-server-config this-server-cfg
                      :server-configs server-configs-map
                      :port (:port this-server-cfg))))

(defn add-leader-change-fn
  [{:keys [this-server join? leader-change-fn] :as raft-config}]
  (let [raft-initialized-chan (async/promise-chan)
        leader-change-fn*     (raft-helpers/leader-change-fn this-server raft-initialized-chan join? leader-change-fn)]
    (assoc raft-config :raft-initialized-chan raft-initialized-chan
                       :leader-change-fn leader-change-fn*)))

(defn path-safe-server-address
  "For creating part of a default log path if not specified in config,
  we use the multi-address of the server, but we want convert the `/`
  to `-` so that we don't create nested directories which isn't needed
  here."
  [server-address]
  (-> server-address
      (str/replace #"^/" "") ;; strip leading /
      (str/replace #"/$" "") ;; strip trailing /
      (str/replace "/" "-"))) ;; replace / with -

(defn add-trailing-slash
  "Adds a '/' to end of string path if it doesn't already exist."
  [path]
  (if (str/ends-with? path "/")
    path
    (str path "/")))

(defn canonicalize-directories
  [{:keys [log-path conn-storage-path this-server] :as raft-config}]
  (log/warn "LOG PATH: " log-path)
  (log/warn "CONN STORAGE PATH: " conn-storage-path)
  (let [log-path*          (-> (or (some-> log-path add-trailing-slash)
                                   (str "./data/" (path-safe-server-address this-server) "/_log/"))
                               io-file/canonicalize-path)
        conn-storage-path* (-> (or conn-storage-path
                                   (str "./data/" (path-safe-server-address this-server) "/"))
                               io-file/canonicalize-path)]
    (log/warn "LOG PATH*: " log-path*)
    (log/warn "CONN STORAGE PATH*: " conn-storage-path*)
    (assoc raft-config :log-path log-path*
                       :log-directory log-path* ;; note raft library config is `:log-directory`
                       :conn-storage-path conn-storage-path*)))


(defn is-shared-storage?
  "If conn method is :file or :memory, returns false, else true."
  [conn]
  (let [method (conn-proto/-method conn)]
    (if (#{:file :memory} method)
      false
      true)))

(defn start
  [handler {:keys [log-history entries-max shared-storage catch-up-rounds fluree/conn]
            :or   {log-history     10
                   entries-max     50
                   catch-up-rounds 10}
            :as   raft-config}]
  (let [shared-storage?        (if (some? shared-storage)
                                 (boolean shared-storage)
                                 (is-shared-storage? conn))
        raft-config*           (-> raft-config
                                   (assoc :event-chan (async/chan)
                                          :command-chan (async/chan)
                                          :send-rpc-fn raft-helpers/send-rpc
                                          :log-history log-history
                                          :entries-max entries-max
                                          :catch-up-rounds catch-up-rounds
                                          :shared-storage? shared-storage?)
                                   (add-server-configs)
                                   (canonicalize-directories)
                                   (add-leader-change-fn)
                                   (add-ledger-storage-fns)
                                   (add-state-machine handler)
                                   (add-snapshot-config))
        _                      (log/debug "Starting Raft with config:" raft-config*)
        raft                   (raft/start raft-config*)
        client-message-handler (partial raft-helpers/message-consume raft (:storage-ledger-read raft-config*))
        new-client-handler     (fn [client]
                                 (ftcp/monitor-remote-connection (:this-server raft-config*) client client-message-handler nil))
        ;; start TCP server, returns the close function
        server-shutdown-fn     (ftcp/start-tcp-server (:port raft-config*) new-client-handler)
        ;; launch client connections on TCP server
        _                      (raft-helpers/launch-network-connections raft-config* raft)
        close-fn               (raft-helpers/close-everything-fn raft (:this-server-config raft-config*) server-shutdown-fn)
        final-map              (-> raft-config*
                                   (select-keys [:this-server :this-server-config :server-configs :storage-ledger-read
                                                 :join? :event-chan :command-chan :private-keys])
                                   (assoc :raft raft
                                          :close close-fn
                                          :server-shutdown server-shutdown-fn ;; TODO - since shutdown of this happens in the :close function below, does this need to remain in this map for anything downstream?

                                          ;; TODO - these following keyword renaming from config's should be updated downstream to use the same names so renaming didn't need to happen
                                          :state-atom (:state-machine-atom raft-config*)
                                          :raft-initialized (:raft-initialized-chan raft-config*)))] ;; added in (add-leader-change-fn ...)


    (raft-helpers/map->RaftGroup final-map)))
