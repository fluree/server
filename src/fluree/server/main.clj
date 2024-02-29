(ns fluree.server.main
  (:require [aero.core :as aero]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [donut.system :as ds]
            [fluree.db.util.log :as log]
            [fluree.server.components.consensus :as consensus]
            [fluree.server.components.fluree :as fluree]
            [fluree.server.components.http :as http]
            [fluree.server.components.subscriptions :as subscriptions]
            [fluree.server.components.watcher :as watcher]
            [fluree.server.components.migrate :as migrate]
            [jsonista.core :as json])
  (:import (java.io PushbackReader))
  (:gen-class))

(set! *warn-on-reflection* true)

(defmethod aero/reader 'include-json-or-edn
  [_opts _tag value]
  (when value
    (cond
      (str/ends-with? value ".edn")
      (with-open [r (io/reader value)]
        (log/debug "Reading EDN file at" value)
        (-> r (PushbackReader.) edn/read))

      (str/ends-with? value ".json")
      (with-open [r (io/reader value)]
        (log/debug "Reading JSON file at" value)
        (json/read-value r)))))

(defn env-config [& [profile]]
  (aero/read-config (io/resource "config.edn")
                    (when profile {:profile profile})))

(def base-system
  {::ds/defs
   {:env    {:http/server       {}
             :fluree/connection {}
             :fluree/consensus  {}}
    :fluree {:conn          fluree/conn
             :consensus     consensus/consensus
             :watcher       watcher/watcher
             :subscriptions subscriptions/subscriptions}
    :http   {:server  http/server
             :handler #::ds{:start  (fn [{{:keys [fluree/connection fluree/consensus fluree/watcher fluree/subscriptions] :as cfg
                                           {:keys [routes middleware]} :http}
                                          ::ds/config}]
                                      (log/debug "ds/config:" cfg)
                                      (http/app {:fluree/conn          connection
                                                 :fluree/consensus     consensus
                                                 :fluree/watcher       watcher
                                                 :fluree/subscriptions subscriptions
                                                 :http/routes          routes
                                                 :http/middleware      middleware}))
                            :config {:http                 (ds/ref [:env :http/server])
                                     :fluree/connection    (ds/ref [:fluree :conn])
                                     :fluree/watcher       (ds/ref [:fluree :watcher])
                                     :fluree/consensus     (ds/ref [:fluree :consensus])
                                     :fluree/subscriptions (ds/ref [:fluree :subscriptions])}}}}})

(defmethod ds/named-system :base
  [_]
  base-system)

(defmethod ds/named-system :dev
  [_]
  (let [ec (env-config :dev)]
    (log/info "dev config:" (pr-str ec))
    (ds/system :base {[:env] ec})))

(defmethod ds/named-system ::ds/repl
  [_]
  (ds/system :dev))

(defmethod ds/named-system :prod
  [_]
  (ds/system :base {[:env] (env-config :prod)}))

(defmethod ds/named-system :docker
  [_]
  (ds/system :prod {[:env] (env-config :docker)}))

(defmethod ds/named-system :migrate
  [_]
  (ds/system {[:http]     ::disabled
              [:migrator] migrate/sid-migrator}))

(defmethod ds/named-system :migrate/prod
  [_]
  (ds/system :migrate {[:env] (env-config :prod)}))

(defmethod ds/named-system :migrate/docker
  [_]
  (ds/system :migrate {[:env] (env-config :docker)}))

(defmethod ds/named-system :migrate/dev
  [_]
  (ds/system :migrate {[:env] (env-config :dev)}))

(defn run-server
  "Runs an HTTP API server in a thread, with :profile from opts or :dev by
  default. Any other keys in opts override config from the profile.
  Returns a zero-arity fn to shut down the server."
  [{:keys [profile] :or {profile :dev} :as opts}]
  (let [cfg-overrides (dissoc opts :profile)
        overide-cfg   #(merge-with merge % cfg-overrides)
        _             (log/debug "run-server cfg-overrides:" cfg-overrides)
        system        (ds/start profile overide-cfg)]
    #(ds/stop system)))

(defn -main
  [& args]
  (let [first-arg (first args)]
    (if (= first-arg "migrate")
      (let [second-arg (second args)
            profile    (if second-arg
                         (keyword first-arg second-arg)
                         :migrate/prod)]
        (log/info "Migrating ledgers with profile:" profile)
        (ds/start profile))
      (let [profile (or (keyword first-arg)
                        :prod)]
        (log/info "Starting fluree/server with profile:" profile)
        (ds/start profile)))))
