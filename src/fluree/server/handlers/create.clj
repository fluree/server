(ns fluree.server.handlers.create
  (:require
   [fluree.db.constants :as const]
   [fluree.db.json-ld.api :as fluree]
   [fluree.db.util.context :as ctx-util]
   [fluree.db.util.core :as util]
   [fluree.db.util.log :as log]
   [fluree.json-ld.processor.api :as jld-processor]
   [fluree.server.consensus :as consensus]
   [fluree.server.handlers.shared :refer [deref! defhandler derive-tx-id wrap-consensus-response]]))

(set! *warn-on-reflection* true)

(defn create-ledger!
  [consensus watcher expanded-txn opts]
  (let [ledger-id (-> expanded-txn (get const/iri-ledger) (get 0) (get "@value"))
        tx-id     (derive-tx-id expanded-txn)
        resp-ch   (consensus/queue-new-ledger consensus watcher ledger-id tx-id expanded-txn opts)]
    (wrap-consensus-response ledger-id tx-id resp-ch)))

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
