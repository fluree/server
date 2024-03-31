(ns fluree.server.components.migrate
  (:require [clojure.core.async :as async :refer [<! >! go-loop]]
            [donut.system :as ds]
            [fluree.db.json-ld.migrate.sid :as sid]
            [fluree.db.nameservice.core :as nameservice]
            [fluree.db.util.async :refer [<?? <? go-try]]
            [fluree.db.util.core :as util]
            [fluree.db.util.log :as log]
            [fluree.server.consensus.raft.producers.new-index-file :as new-index-file]))

;; TODO: this function is unused, but would be necessary if migrating through
;; consensus
(defn push-migrated-index-files
  [consensus index-files-ch resp-ch]
  (go-loop []
    (let [next-file-event (<! index-files-ch)]
      ;; if chan is closed, will stop monitoring loop
      (if next-file-event
        (do (log/debug "About to push new index file event through consensus: " next-file-event)
            (if (util/exception? next-file-event)
              (do (log/error next-file-event "Error in push-new-index-files monitoring new index files, but got an exception.")
                  (when resp-ch
                    (>! resp-ch next-file-event)))
              (let [resp @(new-index-file/push-index-file consensus next-file-event)]
                (when resp-ch
                  (>! resp-ch resp))
                (recur))))
        (when resp-ch
          (async/close! resp-ch))))))

(defn log-migrated-index-files
  "Takes and logs any files from the `index-files-ch`"
  [index-files-ch error-ch]
  (go-loop []
    (when-let [next-file-event (<! index-files-ch)]
      (if (util/exception? next-file-event)
        (do (log/error next-file-event "Error storing index file")
            (when error-ch
              (>! error-ch next-file-event)))
        (do (log/debug "Migrated new index file:" (:address next-file-event))
            (recur))))))

(defn migrate-alias
  [conn commit-opts index-files-ch alias]
  (go-try
    (let [address (<? (nameservice/primary-address conn alias nil))]
      (log/info "Migrating ledger" alias "at address" address)
      (<? (sid/migrate conn address commit-opts index-files-ch)))))

(defn sid-migrate-ledgers
  [conn commit-opts ledgers]
  (go-try
    (loop [[alias & r] ledgers]
      (let [index-files-ch (async/chan)
            error-ch       (async/chan)]
        (log-migrated-index-files index-files-ch error-ch)
        (if alias
          (let [migrate-ch (migrate-alias conn commit-opts index-files-ch alias)]
            (async/alt!
              error-ch   ([e]
                          (throw e))

              migrate-ch ([_ledger]
                          (log/info "Ledger" alias "migrated successfully")))
            (recur r))
          (log/info "All ledgers migrated successfully"))))))

(def sid-migrater
  #::ds{:config {:fluree/connection  (ds/ref [:fluree :conn])
                 :fluree/migrater    (ds/ref [:env :fluree/migrater])
                 :fluree/commit-opts (ds/ref [:env :fluree/commit-options])}
        :start  (fn [{{:keys [fluree/connection fluree/migrater
                              fluree/commit-opts]}
                      ::ds/config}]
                  (let [{:keys [ledgers]} migrater]
                    (log/info "Beginning migration for ledger aliases:" ledgers)
                    (<?? (sid-migrate-ledgers connection commit-opts ledgers))))})
