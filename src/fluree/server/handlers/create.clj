(ns fluree.server.handlers.create
  (:require
   [clojure.core.async :as async :refer [go <!]]
   [fluree.db.api :as fluree]
   [fluree.db.api.transact :as transact-api]
   [fluree.db.util.context :as ctx-util]
   [fluree.db.util.core :as util]
   [fluree.db.util.log :as log]
   [fluree.server.consensus :as consensus]
   [fluree.server.consensus.watcher :as watcher]
   [fluree.server.handlers.shared :refer [deref! defhandler]]
   [fluree.server.handlers.transact :refer [derive-tx-id]]))

(set! *warn-on-reflection* true)

(defn queue-consensus
  [consensus watcher ledger tx-id txn opts]
  (let [;; initial response is not completion, but acknowledgement of persistence
        persist-resp-ch (consensus/queue-new-ledger consensus ledger tx-id txn opts)]

    (go
      (let [persist-resp (<! persist-resp-ch)]
        ;; check for exception trying to put txn in consensus, if so we must deliver the
        ;; watch here, but if successful the consensus process will deliver the watch downstream
        (when (util/exception? persist-resp)
          (watcher/deliver-error watcher tx-id persist-resp))))))

(defn create-ledger
  [consensus watcher txn opts]
  (let [p             (promise)
        ledger-id     (transact-api/extract-ledger-id txn)
        tx-id         (derive-tx-id txn)
        final-resp-ch (watcher/create-watch watcher tx-id)]

    ;; register ledger creation into consensus
    (queue-consensus consensus watcher ledger-id tx-id txn opts)

    ;; wait for final response from consensus and deliver to promise
    (go
      (let [final-resp (<! final-resp-ch)]
        (log/debug "HTTP API ledger creation final response: " final-resp)
        (cond
          (= :timeout final-resp)
          (deliver p (ex-info
                      (str "Timeout waiting for ledger creation to complete for: "
                           ledger-id " with tx-id: " tx-id)
                      {:status 408 :error :db/response-timeout}))

          (nil? final-resp)
          (deliver p (ex-info
                      (str "Unexpected close waiting for ledger creation to complete for: "
                           ledger-id " with tx-id: " tx-id
                           ". Transaction may have processed, check ledger for confirmation.")
                      {:status 500 :error :db/response-closed}))

          :else
          (let [{:keys [ledger-id commit t tx-id]} final-resp]
            (log/info "Ledger created:" ledger-id)
            (deliver p {:ledger ledger-id
                        :commit commit
                        :t      t
                        :tx-id  tx-id})))))
    p))

(defn throw-ledger-exists
  [ledger]
  (let [err-message (str "Ledger " ledger " already exists")]
    (throw (ex-info err-message
                    {:response {:status 409
                                :body   {:error err-message}}}))))

(defhandler default
  [{:keys [fluree/conn fluree/consensus fluree/watcher]
    {:keys [body]} :parameters}]
  (log/debug "create body:" body)
  (let [txn-context    (ctx-util/txn-context body)
        ledger-id      (transact-api/extract-ledger-id body)]
    (if-not (deref! (fluree/exists? conn ledger-id))
      (let [resp-p (create-ledger consensus watcher body {:context txn-context})]
        {:status 201, :body (deref! resp-p)})
      (throw-ledger-exists ledger-id))))
