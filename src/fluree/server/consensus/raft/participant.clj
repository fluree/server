(ns fluree.server.consensus.raft.participant
  (:require [fluree.raft :as raft]
            [fluree.raft.leader :as raft-leader]))

(defn this-server
  "Returns current server's name."
  [raft-state]
  (:this-server raft-state))

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
     (if (raft-leader/is-leader? raft-state)
       (raft/new-entry raft-stub entry callback (or timeout-ms 5000))
       (throw (ex-info (str "This server is no longer the leader! "
                            "Unable to execute command: " command)
                       {:status 400
                        :error  :consensus/not-leader})))
     p)))
