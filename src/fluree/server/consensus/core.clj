(ns fluree.server.consensus.core)

(set! *warn-on-reflection* true)

(defn this-server
  "Returns current server's name."
  [raft-state]
  (:this-server raft-state))
