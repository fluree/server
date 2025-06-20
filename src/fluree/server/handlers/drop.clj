(ns fluree.server.handlers.drop
  (:require [fluree.db.util.log :as log]
            [fluree.server.consensus :as consensus]
            [fluree.server.handlers.shared :refer [deref! defhandler]]
            [fluree.server.watcher :as watcher]))

(set! *warn-on-reflection* true)

(defn queue-consensus
  [consensus watcher ledger-id]
  (let [drop-resp-ch (consensus/queue-drop-ledger consensus ledger-id)]
    (consensus/monitor-consensus watcher ledger-id drop-resp-ch)))

(defn drop-ledger
  [consensus watcher ledger-id]
  (let [p         (promise)
        result-ch (watcher/create-watch watcher ledger-id)]
    (queue-consensus consensus watcher ledger-id)
    (watcher/monitor p ledger-id result-ch)
    p))

(defhandler drop-handler
  [{:keys          [fluree/consensus fluree/watcher]
    {:keys [body]} :parameters}]
  (log/debug "drop body:" body)
  (let [ledger-id (:ledger body)
        resp-p    (drop-ledger consensus watcher ledger-id)]
    {:status 200, :body (deref! resp-p)}))
