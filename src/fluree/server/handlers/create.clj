(ns fluree.server.handlers.create
  (:require
    [clojure.core.async :as async]
    [fluree.db.json-ld.api :as fluree]
    [fluree.db.util.core :as util]
    [fluree.db.util.log :as log]
    [fluree.db.conn.proto :as conn-proto]
    [fluree.server.components.watcher :as watcher]
    [fluree.server.consensus.core :as consensus]
    [fluree.server.handlers.ledger :refer [error-catching-handler deref!]]
    [fluree.server.handlers.transact :refer [normalize-txn]]))

(set! *warn-on-reflection* true)


(defn queue-consensus
  [consensus conn watcher ledger tx-id txn* opts]
  (let [conn-type       (conn-proto/-method conn)
        ;; initial response is not completion, but acknowledgement of persistence
        persist-resp-ch (consensus/queue-new-ledger consensus conn-type ledger tx-id txn* opts)]

    (async/go
      (let [persist-resp (async/<! persist-resp-ch)]
        ;; check for exception trying to put txn in consensus, if so we must deliver the
        ;; watch here, but if successful the consensus process will deliver the watch downstream
        (when (util/exception? persist-resp)
          (watcher/deliver-watch watcher tx-id persist-resp))))))


(defn create-ledger
  [p consensus conn watcher {:keys [ledger txn defaultContext opts]}]
  (let [[tx-id txn*] (normalize-txn txn)
        opts*         (cond-> (or opts {})
                              defaultContext (assoc :defaultContext defaultContext))
        final-resp-ch (watcher/create-watch watcher tx-id)]

    ;; register ledger creation into consensus
    (queue-consensus consensus conn watcher ledger tx-id txn* opts*)

    ;; wait for final response from consensus and deliver to promise
    (async/go
      (let [final-resp (async/<! final-resp-ch)]
        (log/debug "HTTP API ledger creation final response: " final-resp)
        (cond
          (= :timeout final-resp)
          (deliver p (ex-info
                       (str "Timeout waiting for ledger creation to complete for: " ledger " with tx-id: " tx-id)
                       {:status 408 :error :db/response-timeout}))

          (nil? final-resp)
          (deliver p (ex-info
                       (str "Unexpected close waiting for ledger creation to complete for: " ledger
                            " with tx-id: " tx-id ". Transaction may have processed, check ledger for confirmation.")
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


(def default
  (error-catching-handler
    (fn [{:keys                             [fluree/conn fluree/consensus fluree/watcher credential/did]
          {{:keys [ledger] :as body} :body} :parameters}]
      (log/info "Request to create ledger: " ledger)
      (let [_exists? (or (not (deref! (fluree/exists? conn ledger)))
                         (throw-ledger-exists ledger))
            resp-p   (promise)]

        ;; kick of async process that will eventually deliver resp or exception to resp-p
        (create-ledger resp-p consensus conn watcher body)

        {:status 201
         :body   (deref! resp-p)}))))
