(ns user
  (:require [clojure.string :as str]
            [fluree.server.consensus.raft]
            [integrant.repl :refer [clear go halt init reset reset-all]]))

;; Three Server Configuration
(def server-1 "/ip4/127.0.0.1/tcp/62071")
(def server-2 "/ip4/127.0.0.1/tcp/62072")
(def server-3 "/ip4/127.0.0.1/tcp/62073")
(def servers-str (str/join "," [server-1 server-2 server-3]))

(def server-1-overrides
  {:http/jetty        {:port 58090}
   :fluree/connection {:storage-path "data/srv1"}
   :fluree/consensus  {:servers servers-str
                       :this-server server-1}})

(def server-2-overrides
  {:http/server       {:port 58091}
   :fluree/connection {:storage-path "data/srv2"}
   :fluree/consensus  {:consensus-servers     servers-str
                       :consensus-this-server server-2}})

(def server-3-overrides
  {:http/server       {:port 58092}
   :fluree/connection {:storage-path "data/srv3"}
   :fluree/consensus  {:consensus-servers     servers-str
                       :consensus-this-server server-3}})

(def query-server-1-overrides
  {:http/server {:port 58095}
   :fluree/connection {:method :remote
                       :servers "http://localhost:58090"}
   :fluree/consensus {:consensus-type :none}})
