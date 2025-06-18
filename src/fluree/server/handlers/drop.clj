(ns fluree.server.handlers.drop
  (:require [clojure.core.async :as async :refer [<! go]]
            [fluree.db.util.core :as util]
            [fluree.db.util.log :as log]
            [fluree.server.consensus :as consensus]
            [fluree.server.consensus.events :as events]
            [fluree.server.handlers.shared :refer [deref! defhandler]]
            [fluree.server.watcher :as watcher]))

(set! *warn-on-reflection* true)

(defn monitor-consensus
  [watcher ledger-id drop-resp-chan]
  (go
    (let [drop-resp (<! drop-resp-chan)]
      (when (util/exception? drop-resp)
        (log/warn drop-resp "Error dropping ledger")
        (let [error-event (events/error ledger-id drop-resp)]
          (watcher/deliver-event watcher ledger-id error-event))))))

(defn queue-consensus
  [consensus watcher ledger-id]
  (let [drop-resp-ch (consensus/queue-drop-ledger consensus ledger-id)]
    (monitor-consensus watcher ledger-id drop-resp-ch)))

(defn monitor-watch
  [out-p ledger-id result-ch]
  (go
    (let [result (async/<! result-ch)]
      (cond (= :timeout result)
            (deliver out-p (ex-info "Timeout waiting for ledger drop."
                                    {:status 408
                                     :error :ledger/drop-timeout
                                     :ledger ledger-id}))

            (util/exception? result)
            (deliver out-p result)

            (events/error? result)
            (deliver out-p (ex-info (:error-message result) (:error-data result)))

            :else
            (deliver out-p {:ledger ledger-id})))))

(defn drop-ledger
  [consensus watcher ledger-id]
  (let [p         (promise)
        result-ch (watcher/create-watch watcher ledger-id)]
    (queue-consensus consensus watcher ledger-id)
    (monitor-watch p ledger-id result-ch)
    p))

(defhandler drop-handler
  [{:keys          [fluree/consensus fluree/watcher]
    {:keys [body]} :parameters}]
  (log/debug "drop body:" body)
  (let [ledger-id (:ledger body)
        resp-p    (drop-ledger consensus watcher ledger-id)]
    {:status 200, :body (deref! resp-p)}))
