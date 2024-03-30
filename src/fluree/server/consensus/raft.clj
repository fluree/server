(ns fluree.server.consensus.raft
  (:require [clojure.core.async :as async :refer [<! <!! go go-loop]]
            [clojure.pprint :as cprint]
            [clojure.string :as str]
            [fluree.db.util.async :refer [go-try <? <??]]
            [fluree.db.util.log :as log]
            [fluree.raft :as raft]
            [fluree.raft.leader :as raft-leader]
            [fluree.server.consensus.network.tcp :as ftcp]
            [fluree.server.consensus :as consensus :refer [TxGroup]]
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
  [raft _key-storage-read-fn conn message]
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

(defn get-raft-state-async
  "Returns current raft state as a core async channel."
  [raft]
  (let [resp-chan (async/promise-chan)]
    (raft/get-raft-state (:raft raft)
                         (fn [rs]
                           (async/put! resp-chan rs)
                           (async/close! resp-chan)))
    resp-chan))

(defn monitor-raft
  "Monitor raft events and state for debugging"
  ([raft] (monitor-raft raft (fn [x] (cprint/pprint x))))
  ([raft callback]
   (raft/monitor-raft (:raft raft) callback)))

(defn monitor-raft-stop
  "Stops current raft monitor"
  [raft]
  (raft/monitor-raft (:raft raft) nil))

(defn state
  [raft]
  (let [state (<!! (get-raft-state-async raft))]
    (if (instance? Throwable state)
      (throw state)
      state)))

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

(defn leader-new-command!
  "Issue a new command but only if currently the leader, else returns an exception.

  This is used in the consensus handler to issue additional raft commands after doing
  some work.

  Returns a promise channel, which could contain an exception so be sure to check!"
  ([config command params] (leader-new-command! config command params 5000))
  ([{:keys [:consensus/command-chan :consensus/raft-state] :as _config} command params timeout-ms]
   (let [entry     [command params]
         raft-stub {:config {:command-chan command-chan}}
         p         (promise)
         callback  (fn [resp]
                     (deliver p resp))]
     (if (fluree.raft.leader/is-leader? raft-state)
       (raft/new-entry raft-stub entry callback (or timeout-ms 5000))
       (throw (ex-info (str "This server is no longer the leader! "
                            "Unable to execute command: " command)
                       {:status 400
                        :error  :consensus/not-leader})))
     p)))

(defn add-server-async
  "Sends a command to the leader. If no callback provided, returns a core async promise channel
  that will eventually contain a response."
  ([group newServer] (add-server-async group newServer 5000))
  ([group newServer timeout-ms]
   (let [resp-chan (async/promise-chan)
         callback  (fn [resp]
                     (if (nil? resp)
                       (async/close! resp-chan)
                       (async/put! resp-chan resp)))]
     (add-server-async group newServer timeout-ms callback)
     resp-chan))
  ([group newServer timeout-ms callback]
   (go-try (let [raft'  (:raft group)
                 leader (<? (leader-async group))
                 id     (str (UUID/randomUUID))]
             (if (= (:this-server raft') leader)
               (let [command-chan (-> group :command-chan)]
                 (async/put! command-chan [:add-server [id newServer] callback]))
               (do (raft/register-callback raft' id timeout-ms callback)
                   ;; send command to leader
                   (send-rpc raft' leader :add-server [id newServer] nil)))))))

(defn remove-server-async
  "Sends a command to the leader. If no callback provided, returns a core async promise channel
  that will eventually contain a response."
  ([group server] (remove-server-async group server 5000))
  ([group server timeout-ms]
   (let [resp-chan (async/promise-chan)
         callback  (fn [resp]
                     (if (nil? resp)
                       (async/close! resp-chan)
                       (async/put! resp-chan resp)))]
     (remove-server-async group server timeout-ms callback)
     resp-chan))
  ([group server timeout-ms callback]
   (go-try (let [raft'  (:raft group)
                 leader (<? (leader-async group))
                 id     (str (UUID/randomUUID))]
             (if (= (:this-server raft') leader)
               (let [command-chan (-> group :command-chan)]
                 (async/put! command-chan [:remove-server [id server] callback]))
               (do (raft/register-callback raft' id timeout-ms callback)
                   ;; send command to leader
                   (send-rpc raft' leader :remove-server [id server] nil)))))))

(defn local-state
  "Returns local, current state from state machine"
  [raft]
  @(:state-atom raft))

(defrecord RaftGroup [state-atom event-chan command-chan this-server port
                      close raft raft-initialized open-api private-keys]
  TxGroup
  (-add-server-async [group server] (add-server-async group server))
  (-remove-server-async [group server] (remove-server-async group server))
  (-new-entry-async [group entry] (new-entry-async group entry))
  (-local-state [group] (local-state group))
  (-current-state [group] (state group))
  (-update-in [_ ks f] (swap! state-atom update-in ks f))
  (-is-leader? [_] (raft-leader/is-leader? @state-atom))
  (-active-servers [group]
    (let [server-map   (consensus/kv-get-in group [:leases :servers])
          current-time (System/currentTimeMillis)]
      (reduce-kv (fn [acc server lease-data]
                   (if (>= (:expire lease-data) current-time)
                     (conj acc server)
                     acc))
                 #{} server-map))))

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
  [{:keys [this-server-config server-configs join? storage-ledger-read] :as _raft-config} raft-instance]
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
        handler-fn        (partial message-consume raft-instance storage-ledger-read)]
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
