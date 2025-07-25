(ns fluree.server.consensus.raft.producers.new-index-file
  (:require [clojure.core.async :as async :refer [<! go-loop]]
            [fluree.db.util :as util]
            [fluree.db.util.log :as log]
            [fluree.server.consensus.raft.participant :as participant]
            [fluree.server.consensus.raft.producers.new-commit :refer [consensus-push-index-commit]]))

(set! *warn-on-reflection* true)

(defn push-index-file
  "Pushes a new index file event through consensus.

  Adds server name to event, as if not the leader *this* server will get the consensus notification.
  When it comes through consensus, and if it is the same as *this* server, we know we don't need to
  write the files as it is already done."
  [{:keys [:consensus/raft-state] :as config} file-event]
  (let [file-event* (assoc file-event :server (participant/this-server raft-state))]
    ;; returns promise
    (participant/leader-new-command!-async config :new-index-file file-event*)))

(defn push-new-index-files
  "Monitors for new index files pushed onto the changes channel.

  Sends them out one at a time for now, but network interaction optimization
  would be likely if we batched them up to a specific size."
  [index-files-ch config]
  (go-loop []
    (let [next-file-event (<! index-files-ch)]
      ;; if chan is closed, will stop monitoring loop
      (when next-file-event
        (log/debug "About to push new index file event through consensus: " next-file-event)
        (if (util/exception? next-file-event)
          (log/error next-file-event "Error in push-new-index-files monitoring new index files, but got an exception.")
          (do
            (case (:event next-file-event)
              :new-index-file (<! (push-index-file config next-file-event))
              :new-commit     (consensus-push-index-commit config (:data next-file-event)))
            (recur)))))))

(defn monitor-chan
  [config]
  (doto (async/chan 512) ; The buffer prevents the consensus group from slowing
                         ; the indexer's progress initially while still allowing
                         ; the group to catch up if it falls too far behind
    (push-new-index-files config)))
