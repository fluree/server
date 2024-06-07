(ns fluree.server.handlers.create
  (:require
   [clojure.core.async :as async :refer [<! go]]
   [fluree.db.constants :as const]
   [fluree.db.json-ld.api :as fluree]
   [fluree.db.util.context :as ctx-util]
   [fluree.db.util.core :as util]
   [fluree.db.util.log :as log]
   [fluree.json-ld.processor.api :as jld-processor]
   [fluree.server.consensus :as consensus]
   [fluree.server.handlers.shared :refer [deref! defhandler]]
   [fluree.server.handlers.transact :refer [derive-tx-id]]))

(set! *warn-on-reflection* true)

(defn create-ledger!
  [consensus watcher expanded-txn opts]
  (let [ledger-id (-> expanded-txn (get const/iri-ledger) (get 0) (get "@value"))
        tx-id     (derive-tx-id expanded-txn)
        p         (promise)]
    (go
      (let [final-resp (<! (consensus/queue-new-ledger consensus watcher ledger-id tx-id expanded-txn opts))]
        (log/trace "HTTP API ledger creation final response: " final-resp)
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
        [expanded-txn] (-> (ctx-util/use-fluree-context body)
                           jld-processor/expand
                           util/sequential)
        ledger-id      (-> expanded-txn (get const/iri-ledger) (get 0) (get "@value"))]
    (if (deref! (fluree/exists? conn ledger-id))
      (throw-ledger-exists ledger-id)
      (let [create-opts {:context txn-context}
            response    (deref! (create-ledger! consensus watcher expanded-txn create-opts))]
        {:status 201
         :body   response}))))
