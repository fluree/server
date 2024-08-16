(ns fluree.server.task
  (:require [fluree.db.util.log :as log]
            [fluree.server.task.migrate-sid :as task.migrate-sid]))

(defmulti run-task (fn [_conn task] (:id task)))

(defmethod run-task :default [_ task] (log/error "Unknown task id." (:id task)))
(defmethod run-task :migrate/sid
  [conn {:keys [ledgers force]}]
  (task.migrate-sid/migrate conn ledgers force))

(defn run
  [conn task]
  (run-task conn task))
