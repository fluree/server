(ns fluree.server.consensus.producers.new-index-file
  (:require [clojure.core.async :as async :refer [<! go-loop]]
            [fluree.db.util.core :as util]
            [fluree.db.util.log :as log]
            [fluree.server.consensus.core :as consensus]
            [fluree.server.consensus.producers.new-commit :refer [consensus-push-index-commit]]
            [fluree.server.consensus.raft.core :as raft-core]))

(set! *warn-on-reflection* true)

(defn push-index-file
  "Pushes a new index file event through consensus.

  Adds server name to event, as if not the leader *this* server will get the consensus notification.
  When it comes through consensus, and if it is the same as *this* server, we know we don't need to
  write the files as it is already done."
  [{:keys [:consensus/raft-state] :as config} file-event]
  (let [file-event* (assoc file-event :server (consensus/this-server raft-state))]
    ;; returns promise
    (raft-core/leader-new-command! config :new-index-file file-event*)))

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
              :new-index-file (push-index-file config next-file-event)
              :new-commit (consensus-push-index-commit config (:data next-file-event)))
            (recur)))))))

(defn monitor-chan
  [config]
  (doto (async/chan)
    (push-new-index-files config)))
