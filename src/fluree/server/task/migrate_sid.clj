(ns fluree.server.task.migrate-sid
  (:require [clojure.core.async :as async :refer [<! >! go-loop]]
            [fluree.db.json-ld.migrate.sid :as migrate-sid]
            [fluree.db.util.async :refer [go-try]]
            [fluree.db.util.core :as util]
            [fluree.db.util.log :as log]))

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
  [conn ledgers force]
  (go-try
    (log/info :migrate/sid "start" (util/current-time-iso) "." ledgers force)
    (loop [[alias & r] ledgers]
      (let [index-files-ch (async/chan)
            error-ch       (async/chan)]
        (log-migrated-index-files index-files-ch error-ch)
        (if alias
          (let [_ (log/info :migrate/sid "ledger" alias "start" (util/current-time-iso))
                migrate-ch (migrate-sid/migrate conn alias nil force index-files-ch)]
            (async/alt!
              error-ch ([e]
                        (throw e))

              migrate-ch ([_ledger]
                          (log/info :migrate/sid "ledger" alias "complete" (util/current-time-iso))))
            (recur r))
          (log/info :migrate/sid "complete" (util/current-time-iso) "."))))))
