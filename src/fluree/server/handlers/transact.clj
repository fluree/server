(ns fluree.server.handlers.transact
  (:require [clojure.core.async :as async :refer [<! go]]
            [fluree.db.constants :as const]
            [fluree.db.json-ld.api :as fluree]
            [fluree.db.util.context :as ctx-util]
            [fluree.db.util.core :as util]
            [fluree.db.util.log :as log]
            [fluree.json-ld.processor.api :as jld-processor]
            [fluree.server.consensus :as consensus]
            [fluree.server.handlers.shared :refer [defhandler deref! derive-tx-id wrap-consensus-response]]))

(set! *warn-on-reflection* true)

(defn transact!
  [consensus watcher expanded-txn opts]
  (let [ledger-id (-> expanded-txn (get const/iri-ledger) (get 0) (get "@value"))
        tx-id     (derive-tx-id (:raw-txn opts))
        resp-ch   (consensus/queue-new-transaction consensus watcher ledger-id tx-id expanded-txn opts)]
    (wrap-consensus-response ledger-id tx-id resp-ch)))

(defn throw-ledger-doesnt-exist
  [ledger]
  (let [err-message (str "Ledger " ledger " does not exist!")]
    (throw (ex-info err-message
                    {:response {:status 409
                                :body   {:error err-message}}}))))

(defhandler default
  [{:keys [fluree/conn fluree/consensus fluree/watcher credential/did raw-txn]
    {:keys [body]} :parameters}]
  (let [txn-context    (ctx-util/txn-context body)
        [expanded-txn] (-> (ctx-util/use-fluree-context body)
                           jld-processor/expand
                           util/sequential)
        ledger-id      (-> expanded-txn (get const/iri-ledger) (get 0) (get "@value"))
        tx-opts        (cond-> {:context txn-context :raw-txn raw-txn}
                         did (assoc :did did))]
    (log/trace "parsed transact req:" expanded-txn)
    (if (deref! (fluree/exists? conn ledger-id))
      (let [response (deref! (transact! consensus watcher expanded-txn tx-opts))]
        {:status 200
         :body   response})
      (throw-ledger-doesnt-exist ledger-id))))
