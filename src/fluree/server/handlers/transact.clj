(ns fluree.server.handlers.transact
  (:require [clojure.core.async :as async :refer [<! go]]
            [fluree.crypto :as crypto]
            [fluree.db.api :as fluree]
            [fluree.db.constants :as const]
            [fluree.db.util.context :as ctx-util]
            [fluree.db.util.core :as util :refer [get-first-value]]
            [fluree.db.util.log :as log]
            [fluree.json-ld :as json-ld]
            [fluree.json-ld.processor.api :as jld-processor]
            [fluree.server.broadcast :as broadcast]
            [fluree.server.consensus :as consensus]
            [fluree.server.consensus.events :as events]
            [fluree.server.watcher :as watcher]
            [fluree.server.handlers.shared :refer [defhandler deref!]]))

(set! *warn-on-reflection* true)

(defn derive-tx-id
  [raw-txn]
  (if (string? raw-txn)
    (crypto/sha2-256 raw-txn :hex :string)
    (-> (json-ld/normalize-data raw-txn)
        (crypto/sha2-256 :hex :string))))

(defn monitor-consensus-persistence
  [watcher ledger tx-id persist-resp-ch]
  (go
    (let [persist-resp (<! persist-resp-ch)]
      ;; check for exception trying to put txn in consensus, if so we must deliver the
      ;; watch here, but if successful the consensus process will deliver the watch downstream
      (when (util/exception? persist-resp)
        (watcher/deliver-error watcher ledger tx-id persist-resp)))))

(defn queue-consensus
  "Register a new commit with consensus"
  [consensus watcher ledger tx-id expanded-txn opts]
  (let [;; initial response is not completion, but acknowledgement of persistence
        persist-resp-ch (consensus/queue-new-transaction consensus ledger tx-id
                                                         expanded-txn opts)]
    (monitor-consensus-persistence watcher ledger tx-id persist-resp-ch)))

(defn monitor-commit
  "Wait for final commit result from consensus on `result-ch`, broadcast to any
  subscribers and deliver response to promise `out-p`"
  [out-p ledger-id tx-id broadcaster result-ch]
  (go
    (let [result (async/<! result-ch)]
      (log/debug "HTTP API transaction final response: " result)
      (cond
        (= :timeout result)
        (let [ex          (ex-info (str "Timeout waiting for transaction to complete for: "
                                        ledger-id " with tx-id: " tx-id)
                                   {:status 408, :error :db/response-timeout})
              error-event (events/error ledger-id tx-id ex)]
          (broadcast/broadcast-error! broadcaster error-event)
          (deliver out-p ex))

        (nil? result)
        (let [ex          (ex-info (str "Missing transaction result for ledger: "
                                        ledger-id " with tx-id: " tx-id
                                        ". Transaction may have processed, check ledger"
                                        " for confirmation.")
                                   {:status 500, :error :db/response-missing})
              error-event (events/error ledger-id tx-id ex)]
          (broadcast/broadcast-error! broadcaster error-event)
          (deliver out-p ex))

        (util/exception? result)
        (let [error-event (events/error ledger-id tx-id result)]
          (broadcast/broadcast-error! broadcaster error-event)
          (deliver out-p result))

        (events/error? result)
        (let [ex (:error result)]
          (broadcast/broadcast-error! broadcaster result)
          (deliver out-p ex))

        :else
        (let [{:keys [ledger-id commit t tx-id]} result]
          (log/info "Transaction completed for:" ledger-id "tx-id:" tx-id
                    "commit head:" commit)
          (case (events/event-type result)
            :transaction-committed (broadcast/broadcast-new-commit! broadcaster result)
            :ledger-created        (broadcast/broadcast-new-ledger! broadcaster result))
          (deliver out-p {:ledger ledger-id
                          :commit commit
                          :t      t
                          :tx-id  tx-id}))))))

(defn transact!
  [consensus watcher broadcaster expanded-txn opts]
  (let [p         (promise)
        ledger-id (get-first-value expanded-txn const/iri-ledger)
        tx-id     (derive-tx-id (:raw-txn opts))
        result-ch (watcher/create-watch watcher tx-id)]
    (queue-consensus consensus watcher ledger-id tx-id expanded-txn opts)
    (monitor-commit p ledger-id tx-id broadcaster result-ch)
    p))

(defn throw-ledger-doesnt-exist
  [ledger]
  (let [err-message (str "Ledger " ledger " does not exist!")]
    (throw (ex-info err-message
                    {:response {:status 409
                                :body   {:error err-message}}}))))

(defhandler default
  [{:keys [fluree/conn fluree/consensus fluree/watcher fluree/broadcaster credential/did raw-txn]
    {:keys [body]} :parameters}]
  (let [txn-context    (ctx-util/txn-context body)
        [expanded-txn] (-> (ctx-util/use-fluree-context body)
                           jld-processor/expand
                           util/sequential)
        ledger-id      (get-first-value expanded-txn const/iri-ledger)]
    (if (deref! (fluree/exists? conn ledger-id))
      (let [opts   (cond-> {:context txn-context :raw-txn raw-txn}
                     did (assoc :did did))
            resp-p (transact! consensus watcher broadcaster expanded-txn opts)]
        {:status 200, :body (deref! resp-p)})
      (throw-ledger-doesnt-exist ledger-id))))
