(ns fluree.server.handlers.create
  (:require
   [clojure.core.async :as async :refer [go <!]]
   [fluree.db.api.transact :as transact-api]
   [fluree.db.constants :as const]
   [fluree.db.json-ld.api :as fluree]
   [fluree.db.util.core :as util]
   [fluree.db.util.log :as log]
   [fluree.db.util.context :as ctx-util]
   [fluree.db.conn.proto :as conn-proto]
   [fluree.json-ld.processor.api :as jld-processor]
   [fluree.server.components.watcher :as watcher]
   [fluree.server.consensus.core :as consensus]
   [fluree.server.handlers.shared :refer [deref! defhandler]]
   [fluree.server.handlers.transact :refer [derive-tx-id]]))

(set! *warn-on-reflection* true)

(defn queue-consensus
  [consensus conn watcher ledger tx-id txn opts]
  (let [conn-type       (conn-proto/-method conn)
        ;; initial response is not completion, but acknowledgement of persistence
        persist-resp-ch (consensus/queue-new-ledger consensus conn-type ledger tx-id txn opts)]

    (go
      (let [persist-resp (<! persist-resp-ch)]
        ;; check for exception trying to put txn in consensus, if so we must deliver the
        ;; watch here, but if successful the consensus process will deliver the watch downstream
        (when (util/exception? persist-resp)
          (watcher/deliver-watch watcher tx-id persist-resp))))))

(defn create-ledger
  [p consensus conn watcher expanded-txn opts]
  (let [ledger-id     (-> expanded-txn (get const/iri-ledger) (get 0) (get "@value"))
        tx-id         (derive-tx-id expanded-txn)
        final-resp-ch (watcher/create-watch watcher tx-id)]

    ;; register ledger creation into consensus
    (queue-consensus consensus conn watcher ledger-id tx-id expanded-txn opts)

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
          (let [{:keys [ledger-id commit-file-meta t tx-id]} final-resp]
            (log/info "Ledger created:" ledger-id)
            (deliver p {:ledger ledger-id
                        :commit (:address commit-file-meta)
                        :t      (- t)
                        :tx-id  tx-id})))))))

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
        [expanded-txn] (-> (ctx-util/use-fluree-context body)
                           jld-processor/expand
                           util/sequential)
        ledger-id      (-> expanded-txn (get const/iri-ledger) (get 0) (get "@value"))
        resp-p         (promise)]
    (or (not (deref! (fluree/exists? conn ledger-id)))
        (throw-ledger-exists ledger-id))
    ;; kick of async process that will eventually deliver resp or exception to resp-p
    (create-ledger resp-p consensus conn watcher expanded-txn {:context txn-context})
    {:status 201
     :body   (deref! resp-p)}))
