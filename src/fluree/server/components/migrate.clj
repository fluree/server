(ns fluree.server.components.migrate
  (:require [clojure.core.async :as async :refer [>!]]
            [donut.system :as ds]
            [fluree.db.util.core :as util]
            [fluree.db.util.async :refer [<?? <? go-try]]
            [fluree.db.json-ld.migrate.sid :as sid]
            [fluree.db.nameservice.core :as nameservice]
            [fluree.server.consensus.producers.new-index-file :as new-index-file]
            [fluree.db.util.log :as log]))

(defn push-migrated-index-files
  "Monitors for new index files pushed onto the changes channel.

  Sends them out one at a time for now, but network interaction optimization
  would be likely if we batched them up to a specific size."
  ([config index-files-ch resp-ch]
   (async/go-loop []
     (let [next-file-event (async/<! index-files-ch)]
       ;; if chan is closed, will stop monitoring loop
       (if next-file-event
         (do (log/debug "About to push new index file event through consensus: " next-file-event)
             (if (util/exception? next-file-event)
               (do (log/error next-file-event "Error in push-new-index-files monitoring new index files, but got an exception.")
                   (when resp-ch
                     (>! resp-ch next-file-event)))
               (let [resp @(new-index-file/push-index-file config next-file-event)]
                 (when resp-ch
                   (>! resp-ch resp))
                 (recur))))
         (when resp-ch
           (async/close! resp-ch)))))))

(defn sid-migrate-ledgers
  [conn consensus-config commit-opts ledgers]
  (go-try
    (let [index-files-ch (async/chan)
          resp-ch        (async/chan)]
      (push-migrated-index-files consensus-config index-files-ch resp-ch)
      (loop [[alias & r] ledgers]
        (if alias
          (let [address (<? (nameservice/primary-address conn alias nil))]
            (log/info "Migrating ledger" alias "at address" address)
            (<? (sid/migrate conn address commit-opts index-files-ch))
            (if-let [_resp (<? resp-ch)]
              (do (log/info "Ledger" alias "migrated successfully")
                  (recur r))
              (log/info "Got no response from consensus group. Stopping migration.")))
          (do (<? resp-ch)
              (log/info "All ledgers migrated successfully")))))))

(def sid-migrater
  #::ds{:config {:fluree/connection       (ds/ref [:fluree :conn])
                 :fluree/consensus-config (ds/ref [:env :fluree/consensus])
                 :fluree/migrater         (ds/ref [:env :fluree/migrater])
                 :fluree/commit-opts      (ds/ref [:env :fluree/commit-options])}
        :start  (fn [{{:keys [fluree/connection fluree/consensus-config fluree/migrater
                             fluree/commit-opts]}
                     ::ds/config}]
                  (<?? (sid-migrate-ledgers connection consensus-config commit-opts
                                            (:ledgers migrater))))})
