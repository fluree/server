(ns fluree.server.consensus.handlers.create-ledger
  (:require [clojure.core.async :as async]
            [fluree.db.json-ld.api :as fluree]
            [fluree.db.util.json :as json]
            [fluree.raft.leader :refer [is-leader?]]
            [fluree.http-api.handlers.ledger :refer [deref! txn-body->opts]]
            [fluree.db.util.log :as log]
            [fluree.server.consensus.core :as consensus]
            [fluree.server.consensus.producers.new-index-file :refer [push-new-index-files]]
            [fluree.server.consensus.raft.core :as raft]))

(set! *warn-on-reflection* true)

(defn verify-doesnt-exist
  [{:keys [:fluree/conn] :as _config}
   {:keys [ledger-id] :as _params}]
  (let [ledger-exists? (deref! (fluree/exists? conn ledger-id))]
    (log/debug "Ledger" ledger-id "exists?" ledger-exists?)
    (if ledger-exists?
      (do
        (log/info (str "Ledger creation being ignored because " ledger-id " already exists. "
                       "If starting a server, this is likely because the consensus "
                       "messages are replaying, and not a reason for concern."))
        false)
      true)))

(defn create-and-commit
  "Returns promise with eventual response, which could be an exception."
  [{:keys [:fluree/conn] :as config}
   {:keys [ledger-id txn context tx-id] :as _params}]
  (log/trace "Creating ledger" ledger-id "with txn: " txn)
  (let [txn'    (json/parse txn false)
        opts    (txn-body->opts {:txn     txn'
                                 :context context})
        ledger  (deref! (fluree/create conn ledger-id opts))
        commit! (fn [db]
                  (let [index-files-ch (async/chan)
                        _              (push-new-index-files config index-files-ch) ;; monitor for new index files and push across network
                        resp           (fluree/commit! ledger db {:file-data?     true
                                                                  :index-files-ch index-files-ch})]
                    (log/debug "New ledger" ledger-id "created with tx-id: " tx-id)
                    resp))]
    ;; following uses :file-data? and will return map with {:keys [db data-file commit-file]}
    (-> ledger
        fluree/db
        (fluree/stage txn' opts)
        deref!
        (commit!))))


(defn push-consensus
  "Pushes create-ledger request in consensus only if leader.

  Returns promise that will have the eventual response once committed."
  [{:keys [:consensus/state-atom :consensus/raft-state :fluree/conn] :as config}
   {:keys [ledger-id tx-id] :as _params}
   {:keys [db data-file-meta commit-file-meta context-file-meta]}]
  (let [created-body {:ledger-id         ledger-id
                      :data-file-meta    data-file-meta
                      :commit-file-meta  commit-file-meta
                      :context-file-meta context-file-meta
                      ;; below is metadata for quickly validating into the state machine, not retained
                      :t                 (:t db) ;; for quickly validating this is the next 'block'
                      :tx-id             tx-id ;; for quickly removing from the queue
                      :server            (consensus/this-server raft-state) ;; for quickly ensuring this server *is* still the leader
                      }]
    ;; returns promise
    (raft/leader-new-command! config :ledger-created created-body)))


(defn processor
  "Processes create-ledger request.

  Only the leader creates new ledgers."
  [{:keys [:consensus/raft-state] :as config} params]
  (when (is-leader? raft-state)
    (if (verify-doesnt-exist config params)
      (->> params
           (create-and-commit config)
           deref!
           (push-consensus config params)
           deref!)
      false)))


(defn handler
  "Adds a new ledger along with its transaction into the state machine in a queue.
  The consensus leader is responsible for creating all new ledgers, and will get
  to it ASAP."
  [{:keys [:consensus/state-atom :consensus/raft-state] :as config} {:keys [ledger-id] :as params}]
  (log/debug (str "Queuing new ledger into state machine with params: " params))
  (try
    (swap! state-atom update-in
           [:create-ledger ledger-id]
           (fn [existing-state]
             (if existing-state
               (throw (ex-info (str "Ledger id is already pending creation: " ledger-id)
                               {:status 400 :error :db/invalid-ledger-name}))
               params)))
    ;; return original parameters for the next step (processing) as applicable
    params
    (catch Exception e
      ; return exception (don't throw) for handler on error
      (log/warn (ex-message e))
      (if (ex-data e)
        e
        (ex-info (str "Unexpected error queuing new ledger: " (ex-message e))
                 {:status 500
                  :error  :db/unexpected-error}
                 e)))))