(ns fluree.server.components.subscriptions
  (:require [donut.system :as ds]
            [fluree.server.subscriptions :as subscriptions]))

(def subscriptions
  #::ds{:start  subscriptions/listen
        :stop   (fn [{::ds/keys [instance]}]
                  (subscriptions/close instance))
        :config {}})
