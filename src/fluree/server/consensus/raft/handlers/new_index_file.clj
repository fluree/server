(ns fluree.server.consensus.raft.handlers.new-index-file
  (:require [fluree.db.connection :as connection]
            [fluree.db.util.async :refer [<??]]
            [fluree.db.util.log :as log]
            [fluree.server.consensus.raft.participant :as participant]))

(set! *warn-on-reflection* true)

(defn processor
  "Stores a new index file at the specified address.

  Only stores if the server is not the current server"
  [{:keys [consensus/raft-state fluree/conn] :as _config}
   {:keys [server address data] :as _params}]
  (future
    (try
      (if (= server (participant/this-server raft-state))
        (log/debug "Consensus: new index file originated from this server, not writing: " address)
        (<?? (connection/replicate-index-node conn address (:json data))))
      (catch Exception e
        (log/error e "Consensus: Unexpected error writing new index file: " (ex-message e))))))

(defn handler
  "Does a no-op and returns params untouched for handling by the processor.

  We don't update raft state currently based on receiving an index file, just
  need to asynchronously store the file once received."
  [_ params]
  (log/trace "Consensus: New index file received: " params)
  params)
