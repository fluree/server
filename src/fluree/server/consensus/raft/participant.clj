(ns fluree.server.consensus.raft.participant
  (:require [clojure.core.async :as async]
            [fluree.raft :as raft]
            [fluree.raft.leader :as raft-leader]))

(defn this-server
  "Returns current server's name."
  [raft-state]
  (:this-server raft-state))

(defn assert-leader
  [{:keys [:consensus/raft-state] :as _config} command]
  (or (raft-leader/is-leader? raft-state)
      (throw (ex-info (str "This server is no longer the leader! "
                           "Unable to execute command: " command)
                      {:status 400
                       :error  :consensus/not-leader}))))

(defn new-command!
  ([{:keys [:consensus/command-chan] :as _config} command params timeout-ms callback]
   (let [entry     [command params]
         raft-stub {:config {:command-chan command-chan}}]
     (raft/new-entry raft-stub entry callback (or timeout-ms 5000)))))

(defn leader-new-command!
  "Issue a new command but only if currently the leader, else returns an exception.

  This is used in the consensus handler to issue additional raft commands after doing
  some work.

  Returns a promise channel, which could contain an exception so be sure to check!"
  ([config command params]
   (leader-new-command! config command params 5000))

  ([config command params timeout-ms]
   (assert-leader config command)
   (let [p         (promise)
         callback  (fn [resp]
                     (deliver p resp))]
     (new-command! config command params timeout-ms callback)
     p)))

(defn leader-new-command!-async
  ([config command params]
   (leader-new-command!-async config command params 5000))

  ([config command params timeout-ms]
   (assert-leader config command)
   (let [ch       (async/chan)
         callback (fn [resp]
                    (async/put! ch resp
                                (fn [_]
                                  (async/close! ch))))]
     (new-command! config command params timeout-ms callback)
     ch)))
