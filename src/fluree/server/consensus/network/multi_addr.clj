(ns fluree.server.consensus.network.multi-addr
  (:require [clojure.string :as str]
            [fluree.db.util.log :as log]))

;; converts string multi-addresses into map with parameters

(def ipv4-regex #"((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])")
(def ipv6-regex #"(([0-9a-fA-F]{1,4}:){7,7}[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,7}:|([0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,5}(:[0-9a-fA-F]{1,4}){1,2}|([0-9a-fA-F]{1,4}:){1,4}(:[0-9a-fA-F]{1,4}){1,3}|([0-9a-fA-F]{1,4}:){1,3}(:[0-9a-fA-F]{1,4}){1,4}|([0-9a-fA-F]{1,4}:){1,2}(:[0-9a-fA-F]{1,4}){1,5}|[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6})|:((:[0-9a-fA-F]{1,4}){1,7}|:)|fe80:(:[0-9a-fA-F]{0,4}){0,4}%[0-9a-zA-Z]{1,}|::(ffff(:0{1,4}){0,1}:){0,1}((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])|([0-9a-fA-F]{1,4}:){1,4}:((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9]))")

(defn invalid-address-shutdown
  "Will log out an invalid address exception and exist the system."
  [error-msg]
  (log/error "Shutting down due to invalid address.")
  (let [error-msg* (str error-msg
                        " Valid format example: /ip4/127.0.0.1/tcp/62071/alias/server1")
        ex         (ex-info error-msg* {:error  :db/invalid-server-address
                                        :status 400})]
    (log/error ex)
    (System/exit 1)))

(defn generate-alias
  "Generates an alias for a server from the hash of the string specification."
  [multi-addr]
  (let [hash (hash multi-addr)]
    (format "%x" hash)))

(defn multi->map
  "Take string base multi-address and converts into map format required by tcp for connections.

  e.g.: /ip4/127.0.0.1/tcp/62071/alias/srv1"
  [multi-addr]
  (log/debug "multi-addr -> map input: " multi-addr)
  (when-not (str/starts-with? multi-addr "/")
    (invalid-address-shutdown
      (str "Error in multi-address type in: " multi-addr)))
  (let [[_ address-type host protocol port alias? alias] (str/split multi-addr #"/")
        port* (try (Integer/parseInt port)
                   (catch Exception _
                     (invalid-address-shutdown
                       (str "Error in multi-address port in: " multi-addr
                            " Port value must be an integer. "
                            "Supplied: " port))))]
    (when-not ({"ip4" "dns4" "ip6" "dns6"} address-type)
      (invalid-address-shutdown
        (str "Error in multi-address type in: " multi-addr
             " Fluree only supports the ipv4, ipv6, dns4 or dns6 at the moment.")))
    (when-not (= "tcp" protocol)
      (invalid-address-shutdown
        (str "Error in multi-address protocol in: " multi-addr
             " Fluree only supports the tcp protocol at the moment.")))
    (when (= "ip6" address-type)
      (when-not (re-matches ipv6-regex host)
        (invalid-address-shutdown
          (str "Error in multi-address ip6 host address in: " multi-addr
               " Invalid ipv6 address."))))
    (when (= "ip4" address-type)
      (when-not (re-matches ipv4-regex host)
        (invalid-address-shutdown
          (str "Error in multi-address ip4 host address in: " multi-addr
               " Invalid ipv4 address."))))
    (when (and alias? (not= "alias" alias?))
      (invalid-address-shutdown
        (str "Error in multi-address ip4 host address in: " multi-addr
             " Fluree only supports 'alias' name as a way of specifying
             a server alias.")))

    (cond-> {:multi-addr multi-addr
             :host       host
             :port       port*}
            alias (assoc :alias alias))))
