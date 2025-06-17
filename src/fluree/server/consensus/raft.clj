(ns fluree.server.consensus.raft
  (:require [clojure.core.async :as async :refer [<! <!! go go-loop]]
            [clojure.pprint :as cprint]
            [clojure.string :as str]
            [fluree.db.util.async :refer [go-try <? <??]]
            [fluree.db.util.log :as log]
            [fluree.raft :as raft]
            [fluree.server.consensus :as consensus]
            [fluree.server.consensus.network.multi-addr :as multi-addr]
            [fluree.server.consensus.network.tcp :as ftcp]
            [fluree.server.consensus.raft.handler :as raft-handler]
            [fluree.server.io.file :as io-file]
            [taoensso.nippy :as nippy])
  (:import (java.util UUID)))

(set! *warn-on-reflection* true)

(defn snapshot-xfer
  "Transfers snapshot from this server as leader, to a follower.
  Will be called with two arguments, snapshot id and part number.
  Initial call will be for part 1, and subsequent calls, if necessary,
  will be for each successive part.

  Must return a snapshot with the following fields
  :parts - how many parts total
  :data - snapshot data

  If multiple parts are returned, additional requests for each part will be
  requested. A snapshot should be broken into multiple parts if it is larger than
  the amount of data you want to push across the network at once."
  [{:keys [path storage-read]}]
  (fn [id _]
    (let [file (str path id ".snapshot")
          ba   (<!! (storage-read file))]
      {:parts 1
       :data  ba})))

(defn snapshot-installer
  "Installs a new snapshot being sent from a different server.
  Blocking until write succeeds. An error will stop RAFT entirely.

  If snapshot-part = 1, should first delete any existing file if it exists (possible to have historic partial snapshot lingering).

  As soon as final part write succeeds, can safely garbage collect any old snapshots on disk except the most recent one."
  [{:keys [path storage-delete storage-write]}]
  (fn [snapshot-map]
    (let [{:keys [snapshot-index snapshot-part snapshot-data]} snapshot-map
          file (str path snapshot-index ".snapshot")]

      ;; NOTE: Currently snapshot-part is always 1 b/c we never send multi-part snapshots.
      ;;   See comment in `snapshot-xfer` for more details. - WSM 2020-09-01
      (when (= 1 snapshot-part)
        ;; delete any old file if exists
        (storage-delete file))

      ;; TODO: If multi-part snapshot transfers are implemented, need to figure out
      ;;   how & when to write out / upload the file here. For local FS, can just
      ;;   add a way to request appending. For something like S3, we will need to
      ;;   do something else like gather all parts locally and then upload or
      ;;   investigate streaming multipart uploads or similar.
      (<?? (storage-write file snapshot-data)))))

(defn snapshot-reify
  "Reifies a snapshot, should populate whatever data is needed into an initialized state machine
  that is used for raft.

  Called with snapshot-id to reify, which corresponds to the commit index the snapshot was taken.
  Should throw if snapshot not found, or unable to parse. This will stop raft."
  [{:keys [path state-atom storage-read]}]
  (fn [snapshot-id]
    (try
      (let [file  (str path snapshot-id ".snapshot")
            _     (log/debug "Reifying snapshot" file)
            data  (<?? (storage-read file))
            state (when data (nippy/thaw data))]
        (log/debug "Read snapshot data:" state)
        (reset! state-atom state))
      (catch Exception e (log/error e "Error reifying snapshot: " snapshot-id)))))

(defn- return-snapshot-id
  "Takes file map (from storage subsystem) and returns log id (typically same
  as start index) from the file name as a long integer."
  [file]
  (when-let [match (re-find #"^([0-9]+)\.snapshot$" (:name file))]
    (Long/parseLong (second match))))

(defn- purge-snapshots
  [{:keys [path storage-list storage-delete max-snapshots]}]
  (let [rm-snapshots (some->> (storage-list path)
                              <!!
                              (keep return-snapshot-id)
                              (sort >)
                              (drop max-snapshots))]
    (when (not-empty rm-snapshots)
      (log/info "Removing snapshots: " rm-snapshots))
    (doseq [snapshot rm-snapshots]
      (let [file (str/join "/" [(str/replace path #"/$" "")
                                (str snapshot ".snapshot")])]
        (storage-delete file)))))

(defn get-raft-state
  "Returns current raft state to callback."
  [raft callback]
  (raft/get-raft-state (:raft raft) callback))

(defn leader-async
  "Returns leader as a core async channel once available.
  Default timeout supplied, or specify one."
  ([raft] (leader-async raft 60000))
  ([raft timeout]
   (let [timeout-time (+ (System/currentTimeMillis) timeout)]
     (go-loop [retries 0]
       (let [resp-chan (async/promise-chan)
             _         (get-raft-state raft (fn [state]
                                              (if-let [leader (:leader state)]
                                                (async/put! resp-chan leader)
                                                (async/close! resp-chan))))
             resp      (<! resp-chan)]
         (cond
           resp resp

           (> (System/currentTimeMillis) timeout-time)
           (ex-info (format "Leader not yet established and timeout of %s reached. Polled raft state %s times." timeout retries)
                    {:status 400 :error :db/leader-timeout})

           :else
           (do
             (<! (async/timeout 100))
             (recur (inc retries)))))))))

(defn is-leader?-async
  [raft]
  (go
    (let [leader (<! (leader-async raft))]
      (if (instance? Throwable leader)
        leader
        (= (:this-server raft) leader)))))

(defn is-leader?
  [raft]
  (let [leader? (<!! (is-leader?-async raft))]
    (if (instance? Throwable leader?)
      (throw leader?)
      leader?)))

(defn snapshot-writer*
  "Blocking until write succeeds. An error will stop RAFT entirely."
  [{:keys [path state-atom storage-write] :as config}]
  (fn [index callback]
    (log/info "Ledger group snapshot write triggered for index:" index)
    (let [start-time    (System/currentTimeMillis)
          state         @state-atom
          file          (str path index ".snapshot")
          max-snapshots 6
          data          (nippy/freeze state)]
      (log/debug "Writing raft snapshot" file "with contents" state)
      (try
        (<?? (storage-write file data))
        (catch Exception e (log/error e "Error writing snapshot index:" index)))
      (log/info (format "Ledger group snapshot completed for index %s in %s milliseconds."
                        index (- (System/currentTimeMillis) start-time)))
      (callback)
      (purge-snapshots (assoc config :max-snapshots max-snapshots)))))

(defn snapshot-writer
  "Wraps snapshot-writer* in the logic that determines whether all nodes or
  only the leader should write snapshots."
  [{:keys [only-leader-snapshots] :as config}]
  (let [writer (snapshot-writer* config)]
    (fn [index callback]
      (if only-leader-snapshots
        (when (is-leader? config)
          (writer index callback))
        (writer index callback)))))

(defn snapshot-list-indexes
  "Lists all stored snapshot indexes, sorted ascending. Used for bootstrapping a
  raft network from a previously made snapshot."
  [{:keys [path storage-list]}]
  (log/debug "Initialized snapshot-list-indexes with path" path "and storage-list" storage-list)
  (fn []
    (log/debug "Listing snapshot indexes in" path)
    (let [files (try
                  (<?? (storage-list path))
                  (catch Exception e (log/error e "Error listing stored snapshots in" path)))]
      (log/debug "Got snapshot candidate files:" files)
      (->> files
           (map :name)
           (keep #(when-let [match (re-find #"^([0-9]+)\.snapshot$" %)]
                    (Long/parseLong (second match))))
           sort
           vec))))

;; Holds state change functions that are registered
(def state-change-fn-atom (atom {}))

(defn register-state-change-fn
  "Registers function to be called with every state monitor change. id provided is used to un-register function
  and is otherwise opaque to the system."
  [id f]
  (swap! state-change-fn-atom assoc id f))

(defn unregister-state-change-fn
  [id]
  (swap! state-change-fn-atom dissoc id))

(defn unregister-all-state-change-fn
  []
  (reset! state-change-fn-atom {}))

;; map of request-ids to a response channel that will contain the response eventually
(def pending-responses (atom {}))

(defn send-rpc
  "Sends rpc call to specified server.
  Includes a resp-chan that will eventually contain a response.

  Returns true if successful, else will return an exception if
  connection doesn't exist (not established, or closed)."
  [raft server operation data callback]
  (let [this-server (:this-server raft)
        msg-id      (str (UUID/randomUUID))
        header      {:op     operation
                     :from   this-server
                     :to     server
                     :msg-id msg-id}]
    (log/trace "send-rpc start:" {:op operation :data data :header header})
    (when (fn? callback)
      (swap! pending-responses assoc msg-id callback))
    (let [success? (ftcp/send-rpc this-server server header data)]
      (if success?
        (log/trace "send-rpc success:" {:op operation :data data :header header})
        (do
          (swap! pending-responses dissoc msg-id)
          (log/debug "Connection to" server "is closed, unable to send rpc." header))))))

(defn message-consume
  "Function used to consume inbound server messages.

  client-id should be an approved client-id from the initial
  client negotiation process, can be used to validate incoming
  messages are labeled as coming from the correct client."
  [raft conn message]
  (try
    (let [message'  (nippy/thaw message)
          [header data] message'
          {:keys [op msg-id]} header
          response? (str/ends-with? (name op) "-response")
          {:keys [write-chan]} conn]
      (if response?
        (let [callback (get @pending-responses msg-id)]
          (when (fn? callback)
            (swap! pending-responses dissoc msg-id)
            (callback data)))
        (let [resp-header (assoc header :op (keyword (str (name op) "-response"))
                                 :to (:from header)
                                 :from (:to header))
              callback    (fn [x]
                            (ftcp/send-message write-chan resp-header x))]
          (case op
            :new-command
            (let [{:keys [id entry]} data
                  command (raft/map->RaftCommand {:entry entry
                                                  :id    id})]
              (log/debug "Raft - new command:" data)
              (raft/new-command raft command callback))

            ;; else
            (raft/invoke-rpc raft op data callback)))))
    (catch Exception e (log/error e "Error consuming new message! Ignoring."))))

;; start with a default state when no other state is present (initialization)
;; useful for setting a base 'version' in state
(def default-state {:version 3})

(defn close-everything-fn
  "Returns a zero-arity function to close down all systems related to the raft network"
  [raft this-server-config server-shutdown]
  (fn []
    ;; close raft
    (raft/close raft)
    ;; Unregister state-change-fns
    (unregister-all-state-change-fn)
    ;; close any open connections
    (ftcp/close-all-connections (:multi-addr this-server-config))
    ;; close tcp server
    (server-shutdown)))

(defn monitor-raft
  "Monitor raft events and state for debugging"
  ([raft] (monitor-raft raft (fn [x] (cprint/pprint x))))
  ([raft callback]
   (raft/monitor-raft (:raft raft) callback)))

(defn monitor-raft-stop
  "Stops current raft monitor"
  [raft]
  (raft/monitor-raft (:raft raft) nil))

;; TODO configurable timeout
(defn new-entry-async
  "Sends a command to the leader. If no callback provided, returns a core async promise channel
  that will eventually contain a response."
  ([group entry] (new-entry-async group entry 5000))
  ([group entry timeout-ms]
   (let [resp-chan (async/promise-chan)
         callback  (fn [resp]
                     (if (nil? resp)
                       (async/close! resp-chan)
                       (async/put! resp-chan resp)))]
     (new-entry-async group entry timeout-ms callback)
     resp-chan))
  ([group entry timeout-ms callback]
   (go-try (let [{:keys [this-server] :as raft} (:raft group)
                 leader (<? (leader-async group))]
             (if (= this-server leader)
               (raft/new-entry raft entry callback timeout-ms)
               (let [id           (str (UUID/randomUUID))
                     command-data {:id id :entry entry}]
                 ;; since not leader, register entry id locally and will receive callback when committed to state machine
                 (raft/register-callback raft id timeout-ms callback)
                 ;; send command to leader
                 (send-rpc raft leader :new-command command-data nil)))))))

(defrecord RaftGroup [state-atom event-chan command-chan this-server port
                      close raft raft-initialized open-api private-keys]
  consensus/Transactor
  (-queue-new-ledger [group ledger-msg]
    (new-entry-async group ledger-msg))
  (-queue-drop-ledger [group ledger-msg]
    (throw (ex-info "Drop not supported for Raft consensus." (merge ledger-msg {:error :not-implemented}))))
  (-queue-new-transaction [group txn-msg]
    (new-entry-async group txn-msg)))

(defn leader-change-fn
  "Function called every time there is a leader change to provide any extra
  logic the system wishes to do when leader changes occur"
  [this-server raft-initialized-chan join? config-leader-change-fn]
  (when (and config-leader-change-fn
             (not (fn? config-leader-change-fn)))
    (throw (ex-info (str "Consensus leader change function supplied, but it is not a function!: "
                         config-leader-change-fn)
                    {:status 500 :error :consensus/config})))

  (fn [change-map]
    (let [{:keys [new-raft-state old-raft-state]} change-map]
      (log/info "Ledger group leader change:"
                (dissoc change-map :key :new-raft-state :old-raft-state))
      (log/trace "Leader change: \n" "... Old raft state: \n" old-raft-state "\n"
                 "... New raft state: \n" new-raft-state)
      (when (not (nil? new-raft-state))
        (cond (and join? (not (nil? (:leader new-raft-state)))
                   (not= this-server (:leader new-raft-state)))
              (async/put! raft-initialized-chan :follower)

              join?
              true

              (= this-server (:leader new-raft-state))
              (async/put! raft-initialized-chan :leader)

              :else
              (async/put! raft-initialized-chan :follower)))
      (when config-leader-change-fn
        (config-leader-change-fn change-map)))))

(defn validated-raft-servers
  "Returns list of raft servers as needed by the raft library
  using the startup server map configs. Does some basic validation."
  [server-configs]
  (let [raft-servers (->> (map :multi-addr server-configs)
                          (into #{})
                          ;; TODO - does downstream really need a vector? seems set should be fine
                          (into []))]

    ;; check that the initial server config did not duplicate the same server name
    (when (not= (count server-configs) (count raft-servers))
      (throw (ex-info (str "There appear to be duplicates in the group servers configuration: "
                           (pr-str server-configs))
                      {:status 400 :error :db/invalid-configuration})))
    raft-servers))

(defn validated-this-server-config
  "Returns 'this' server's config by finding the matching entry of 'this-server'
  in the list of server config maps. Does some basic validation."
  [this-server server-configs]
  (let [this-server-cfg (some #(when (= this-server %) %) server-configs)]
    (when-not this-server-cfg
      (throw (ex-info (str "This server: " (pr-str this-server) " has to be included in the group
                                server configuration." (pr-str server-configs))
                      {:status 400 :error :db/invalid-configuration})))
    this-server-cfg))

(defn launch-network-connections
  "Fires up network and provides it the handler for new events/commands"
  [{:keys [this-server-config server-configs join?] :as _raft-config} raft-instance]
  ;; TODO - Need slightly more complicated handling. If a server joins, close, and tries to restart with join = false, will fail
  (let [all-other-servers (filter #(not= (:multi-addr this-server-config) (:multi-addr %)) server-configs)
        connect-servers   (if join?
                            ;; If joining an existing network, connects to all other servers
                            all-other-servers
                            ;; simple rule (for now) is we connect to servers whose id is > (lexical sort) than our own
                            (filter #(> 0 (compare (:multi-addr this-server-config) (:multi-addr %))) server-configs))
        _                 (log/trace "Launching network connections for this-server-config: " this-server-config
                                     " server-configs: " server-configs
                                     " all-other-servers: " all-other-servers
                                     " connect-servers: " connect-servers)
        handler-fn        (partial message-consume raft-instance)]
    (doseq [connect-to connect-servers]
      (log/debug "Raft: attempting connection to server: " connect-to)
      (ftcp/launch-client-connection this-server-config connect-to handler-fn))))

(defn handler
  "Raft calls the state-machine handler with two args, the first is 'command' which
  includes a two-tuple of [event-name parameters].

  The system-provided handler function take a 3 parameter function of
  [config event parameters]

  Here we create the config map that gets injected, and return a function that breaks apart
  the command into the event + parameters part to then call the config-supplied state function."
  [config-handler config]
  (fn [[command params] raft-state]
    (config-handler (assoc config :consensus/raft-state raft-state) command params)))

(defn build-snapshot-config
  "Returns a map of the necessary configurations for snapshot reading/writing, etc.
  Used automatically by the raft system to handle all snapshot activities."
  [{:keys [encryption-key storage-group-read storage-group-write storage-group-exists
           storage-group-delete storage-group-list log-directory state-machine-atom] :as _raft-config}]
  {:path           "snapshots/"
   :state-atom     state-machine-atom
   :storage-read   (or storage-group-read
                       (io-file/connection-storage-read
                        log-directory
                        encryption-key))
   :storage-write  (or storage-group-write
                       (io-file/connection-storage-write
                        log-directory
                        encryption-key))
   :storage-exists (or storage-group-exists
                       (io-file/connection-storage-exists?
                        log-directory))
   :storage-delete (or storage-group-delete
                       (io-file/connection-storage-delete
                        log-directory))
   :storage-list   (or storage-group-list
                       (io-file/connection-storage-list
                        log-directory))})

(defn add-ledger-storage-fns
  "Functions that write and read ledger data. Either supplied with config, or generated
  automatically with defaults"
  [{:keys [encryption-key ledger-directory
           storage-ledger-read storage-ledger-write] :as raft-config}]
  (assoc raft-config
         :storage-ledger-read (or storage-ledger-read
                                  (io-file/connection-storage-read
                                   ledger-directory
                                   encryption-key))
         :storage-ledger-write (or storage-ledger-write
                                   (io-file/connection-storage-write
                                    ledger-directory
                                    encryption-key))))

(defn add-state-machine
  "Add state machine configuration options needed for raft"
  [{:keys [conn watcher subscriptions server this-server command-chan
           storage-ledger-read storage-ledger-write]
    :as raft-config}
   config-handler]
  (let [state-machine-atom   (atom default-state)
        state-machine-config {:fluree/conn                    conn
                              :fluree/watcher                 watcher
                              :fluree/subscriptions           subscriptions
                              :fluree/server                  server
                              :consensus/command-chan         command-chan
                              :consensus/this-server          this-server
                              :consensus/state-atom           state-machine-atom
                              :consensus/ledger-read          storage-ledger-read
                              :consensus/ledger-write         storage-ledger-write
                              :consensus/state-change-fn-atom state-change-fn-atom}]
    (assoc raft-config
           :state-machine-atom state-machine-atom
           :state-machine (handler config-handler state-machine-config))))

(defn add-snapshot-config
  [raft-config]
  (let [snapshot-config (build-snapshot-config raft-config)]
    (assoc raft-config :snapshot-write (snapshot-writer snapshot-config)
           :snapshot-reify (snapshot-reify snapshot-config)
           :snapshot-xfer (snapshot-xfer snapshot-config)
           :snapshot-install (snapshot-installer snapshot-config)
           :snapshot-list-indexes (snapshot-list-indexes snapshot-config))))

(defn add-server-configs
  [{:keys [this-server servers] :as raft-state}]
  (let [servers*           (mapv str/trim servers)
        this-server*       (if this-server
                             (str/trim this-server)
                             (if (= 1 (count servers*))
                               (first servers*)
                               (throw (ex-info "Must specify this-server if multiple servers are specified"
                                               {:status 400 :error :db/invalid-server-address}))))
        server-configs-map (mapv multi-addr/multi->map servers*)
        this-server-map    (multi-addr/multi->map this-server*)
        this-server-cfg    (validated-this-server-config this-server-map server-configs-map)]
    (validated-raft-servers server-configs-map)
    (assoc raft-state
           :this-server this-server*
           :servers servers*
           :this-server-config this-server-cfg
           :server-configs server-configs-map
           :port (:port this-server-cfg))))

(defn add-leader-change-fn
  [{:keys [this-server join?], change-fn :leader-change-fn :as raft-config}]
  (let [raft-initialized-chan (async/promise-chan)
        leader-change-fn*     (leader-change-fn this-server raft-initialized-chan join? change-fn)]
    (assoc raft-config :raft-initialized-chan raft-initialized-chan
           :leader-change-fn leader-change-fn*)))

(defn default-data-directory
  [server-name directory-type]
  (str/join "/" ["." "data" server-name directory-type ""]))

(defn default-log-directory
  [ledger-directory server-name]
  (if ledger-directory
    (str ledger-directory "/raftlog")
    (default-data-directory server-name "raftlog")))

(defn default-ledger-directory
  [server-name]
  (default-data-directory server-name "ledger"))

(defn canonicalize-directories
  [{:keys [log-directory ledger-directory this-server] :as raft-config}]
  (let [server-name       (name this-server)
        log-directory*    (-> log-directory
                              (or (default-log-directory ledger-directory server-name))
                              io-file/canonicalize-path)
        ledger-directory* (-> ledger-directory
                              (or (default-ledger-directory server-name))
                              io-file/canonicalize-path)]
    (assoc raft-config
           :log-directory log-directory*
           :ledger-directory ledger-directory*)))

(defn start
  [{:keys [log-history entries-max storage-type catch-up-rounds]
    :or   {log-history     10
           entries-max     50
           catch-up-rounds 10}
    :as   raft-config}]
  (let [event-handler-fn       (raft-handler/create-handler raft-handler/default-routes)
        raft-config*           (-> raft-config
                                   (assoc :event-chan (async/chan)
                                          :command-chan (async/chan)
                                          :send-rpc-fn send-rpc
                                          :log-history log-history
                                          :entries-max entries-max
                                          :catch-up-rounds catch-up-rounds
                                          :only-leader-snapshots (not= :file storage-type))
                                   (add-server-configs)
                                   (canonicalize-directories)
                                   (add-leader-change-fn)
                                   (add-ledger-storage-fns)
                                   (add-state-machine event-handler-fn)
                                   (add-snapshot-config))
        _                      (log/debug "Starting Raft with config:" raft-config*)
        raft                   (raft/start raft-config*)
        client-message-handler (partial message-consume raft)
        new-client-handler     (fn [client]
                                 (ftcp/monitor-remote-connection (:this-server raft-config*) client client-message-handler nil))
        ;; start TCP server, returns the close function
        server-shutdown-fn     (ftcp/start-tcp-server (:port raft-config*) new-client-handler)
        ;; launch client connections on TCP server
        _                      (launch-network-connections raft-config* raft)
        close-fn               (close-everything-fn raft (:this-server-config raft-config*) server-shutdown-fn)
        final-map              (-> raft-config*
                                   (select-keys [:this-server :this-server-config :server-configs :storage-ledger-read
                                                 :join? :event-chan :command-chan :private-keys :open-api])
                                   (assoc :raft raft
                                          :close close-fn
                                          :server-shutdown server-shutdown-fn ;; TODO - since shutdown of this happens in the :close function below, does this need to remain in this map for anything downstream?

                                          ;; TODO - these following keyword renaming from config's should be updated downstream to use the same names so renaming didn't need to happen
                                          :state-atom (:state-machine-atom raft-config*)
                                          :raft-initialized (:raft-initialized-chan raft-config*)))] ;; added in (add-leader-change-fn ...)

    (map->RaftGroup final-map)))
