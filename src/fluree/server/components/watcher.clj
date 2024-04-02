(ns fluree.server.components.watcher
  (:refer-clojure :exclude [remove-watch])
  (:require [donut.system :as ds]
            [fluree.server.consensus.watcher :as watcher]))

(def watcher
  #::ds{:start  (fn [{cfg ::ds/config}]
                  (watcher/watch cfg))
        :stop   (fn [{::ds/keys [instance]}]
                  (watcher/close instance))
        :config {:max-tx-wait-ms (ds/ref [:env :http/server :max-tx-wait-ms])}})
