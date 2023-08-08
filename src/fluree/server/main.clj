(ns fluree.server.main
  (:require [clojure.java.io :as io]
            [aero.core :as aero]
            [donut.system :as ds]
            [fluree.db.util.log :as log]
            [fluree.server.components.consensus :as consensus]
            [fluree.server.components.http :as http]
            [fluree.server.components.watcher :as watcher]
            [fluree.server.components.fluree :as fluree])
  (:gen-class))

(set! *warn-on-reflection* true)

(defn env-config [& [profile]]
  (aero/read-config (io/resource "config.edn")
                    (when profile {:profile profile})))


(def base-system
  {::ds/defs
   {:env    {:http/server       {}
             :fluree/connection {}
             :fluree/consensus  {}}
    :fluree {:conn      fluree/conn
             :consensus consensus/consensus
             :watcher   watcher/watcher}
    :http   {:server  http/server
             :handler #::ds{:start  (fn [{{:keys [fluree/connection fluree/consensus fluree/watcher] :as cfg
                                           {:keys [routes middleware]} :http}
                                          ::ds/config}]
                                      (log/debug "ds/config:" cfg)
                                      (http/app {:fluree/conn      connection
                                                 :fluree/consensus consensus
                                                 :fluree/watcher   watcher
                                                 :http/routes      routes
                                                 :http/middleware  middleware}))
                            :config {:http              (ds/ref [:env :http/server])
                                     :fluree/connection (ds/ref [:fluree :conn])
                                     :fluree/watcher    (ds/ref [:fluree :watcher])
                                     :fluree/consensus  (ds/ref [:fluree :consensus])}}}}})

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
  (let [profile (or (-> args first keyword) :prod)]
    (ds/start profile)))
