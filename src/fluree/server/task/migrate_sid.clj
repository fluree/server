(ns fluree.server.task.migrate-sid
  (:require [clojure.core.async :as async :refer [<! >! go-loop]]
            [fluree.db.json-ld.migrate.sid :as migrate-sid]
            [fluree.db.util.async :refer [go-try]]
            [fluree.db.util.core :as util]
            [fluree.db.util.log :as log]
            [malli.core :as m]))

(def MigrateSidTask
  [:map
   [:id [:enum :migrate/sid]]
   [:ledgers {:doc "Collection of ledger aliases to migrate."}
    [:sequential :string]]
   [:force {:optional true
            :doc "If true, run the migration regardless of whether the ledger has already been migrated."}
    :boolean]])

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

(defn migrate
  [conn {:keys [id ledgers force] :as task}]
  (if-let [error (m/explain MigrateSidTask task)]
    (do (log/error "Invalid task configuration." error)
        (throw (ex-info "Invalid task configuration."
                        {:task task :error error})))
    (go-try
      (log/info id "start" (util/current-time-iso) "." task)
      (loop [[alias & r] ledgers]
        (let [index-files-ch (async/chan)
              error-ch       (async/chan)]
          (log-migrated-index-files index-files-ch error-ch)
          (if alias
            (let [_ (log/info id "ledger" alias "start" (util/current-time-iso))
                  migrate-ch (migrate-sid/migrate conn alias nil force index-files-ch)]
              (async/alt!
                error-ch ([e]
                          (throw e))

                migrate-ch ([_ledger]
                            (log/info id "ledger" alias "complete" (util/current-time-iso))))
              (recur r))
            (log/info id "complete" (util/current-time-iso) ".")))))))
