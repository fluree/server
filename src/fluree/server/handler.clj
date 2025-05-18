(ns fluree.server.handler
  (:require [clojure.core.async :as async :refer [<!!]]
            [clojure.string :as str]
            [fluree.db.json-ld.credential :as cred]
            [fluree.db.query.fql.syntax :as fql]
            [fluree.db.query.history.parse :as fqh]
            [fluree.db.util.json :as json]
            [fluree.db.util.log :as log]
            [fluree.db.validation :as v]
            [fluree.server.handlers.create :as create]
            [fluree.server.handlers.ledger :as ledger]
            [fluree.server.handlers.remote-resource :as remote]
            [fluree.server.handlers.subscription :as subscription]
            [fluree.server.handlers.transact :as srv-tx]
            [malli.core :as m]
            [muuntaja.core :as muuntaja]
            [muuntaja.format.core :as mf]
            [muuntaja.format.json :as mfj]
            [reitit.coercion.malli :as rcm]
            [reitit.ring :as ring]
            [reitit.ring.coercion :as coercion]
            [reitit.ring.middleware.exception :as exception]
            [reitit.ring.middleware.muuntaja :as muuntaja-mw]
            [reitit.swagger :as swagger]
            [reitit.swagger-ui :as swagger-ui]
            [ring.adapter.jetty9 :as http]
            [ring.middleware.cors :as rmc])
  (:import (java.io InputStream)))

(set! *warn-on-reflection* true)

(def LedgerAlias
  (m/schema [:string {:min 1}]))

(def LedgerAddress
  (m/schema [:string {:min 1}]))

(def Address
  (m/schema [:string {:min 1}]))

(def TransactOpts
  (m/schema [:map-of :keyword :any]))

(def CreateRequestBody
  (m/schema [:map-of [:orn [:string :string] [:keyword :keyword]] :any]))

(def TValue
  (m/schema pos-int?))

(def DID
  (m/schema [:string {:min 1}]))

(def CommitAddress
  (m/schema [:string {:min 1}]))

(def CommitHash
  (m/schema [:string {:min 1}]))

(def CommitResultMessage
  (m/schema [:map
             [:address CommitAddress]
             [:hash CommitHash]]))

(def CreateResponseBody
  (m/schema [:and
             [:map-of :keyword :any]
             [:map
              [:ledger LedgerAlias]
              [:t TValue]
              [:tx-id DID]
              [:commit CommitResultMessage]]]))

(def SubscriptionRequestBody
  (m/schema [:fn {:error/message {:en "Invalid websocket upgrade request"}}
             http/ws-upgrade-request?]))

(def TransactRequestBody
  (m/schema [:orn
             [:sparql :string]
             [:fql [:map-of :any :any]]]))

(def TransactResponseBody
  (m/schema [:and
             [:map-of :keyword :any]
             [:map
              [:ledger LedgerAlias]
              [:t TValue]
              [:tx-id DID]
              [:commit CommitResultMessage]]]))

(def FqlQuery (m/schema (-> (fql/query-schema [])
                            ;; hack to make query schema open instead of closed
                            ;; TODO: remove once db is updated to open
                            (update 3 (fn [schm] (assoc schm 1 {:closed false}))))
                        {:registry fql/registry}))

(def SparqlQuery (m/schema :string))

(def QueryResponse
  (m/schema [:orn
             [:select [:sequential [:or coll? map?]]]
             [:select-one [:or coll? map?]]
             [:construct map?]]))

(def HistoryQuery
  (m/schema (fqh/history-query-schema [[:from LedgerAlias]])
            {:registry fqh/registry}))

(def QueryFormat (m/schema [:enum :sparql]))

(def QueryRequestBody
  (m/schema [:multi {:dispatch ::format}
             [:sparql [:map
                       [::query SparqlQuery]
                       [::format QueryFormat]]]
             [::m/default FqlQuery]]))

(def HistoryQueryResponse
  (m/schema [:sequential map?]))

(def LatestCommitRequestBody
  (m/schema [:and
             [:map-of :keyword :any]
             [:map
              [:resource LedgerAddress]]]))

(def AddressRequestBody
  (m/schema [:and
             [:map-of :keyword :any]
             [:map
              [:resource Address]]]))

(def AliasRequestBody
  (m/schema [:and
             [:map-of :keyword :any]
             [:map
              [:ledger LedgerAlias]]]))

(def ErrorResponse
  [:or :string map?])

(def query-coercer
  (let [default-transfomers (:transformers rcm/default-options)
        json-transformer    (-> default-transfomers :body :formats (get "application/json"))
        transformers        (-> default-transfomers
                                (assoc-in [:body :formats "application/jwt"] json-transformer)
                                (assoc-in [:body :formats "application/json"] fql/fql-transformer))]
    (rcm/create
     {:transformers transformers
      :strip-extra-keys false
      :error-keys #{}
      :encode-error (fn [explained]
                      {:error :db/invalid-query
                       :message (v/format-explained-errors explained nil)})})))

(def history-coercer
  (let [default-transfomers (:transformers rcm/default-options)
        json-transformer    (-> default-transfomers :body :formats (get "application/json"))
        transformers        (-> default-transfomers
                                (assoc-in [:body :formats "application/jwt"] json-transformer)
                                (assoc-in [:body :default] fql/fql-transformer))]
    (rcm/create
     {:strip-extra-keys false
      :error-keys #{}
      :transformers transformers
      :encode-error (fn [explained]
                      {:error :db/invalid-query
                       :message (v/format-explained-errors explained nil)})})))

(def query-endpoint
  {:summary    "Endpoint for submitting queries"
   :parameters {:body QueryRequestBody}
   :responses  {200 {:body QueryResponse}
                400 {:body ErrorResponse}
                500 {:body ErrorResponse}}
   :coercion   ^:replace  query-coercer
   :handler    #'ledger/query})

(def history-endpoint
  {:summary    "Endpoint for submitting history queries"
   :parameters {:body HistoryQuery}
   :responses  {200 {:body HistoryQueryResponse}
                400 {:body ErrorResponse}
                500 {:body ErrorResponse}}
   :coercion   ^:replace history-coercer
   :handler    #'ledger/history})

(defn wrap-assoc-system
  [conn consensus watcher subscriptions handler]
  (fn [req]
    (-> req
        (assoc :fluree/conn conn
               :fluree/consensus consensus
               :fluree/watcher watcher
               :fluree/subscriptions subscriptions)
        handler)))

(defn wrap-cors
  [handler]
  (rmc/wrap-cors handler
                 :access-control-allow-origin [#".*"]
                 :access-control-allow-methods [:get :post]))

(defn unwrap-credential
  "Checks to see if the request body (:body-params) is a verifiable credential. If it is,
  verify the validity of the signature. If the signature is valid, add the verified
  issuer to the request and unwrap the credential, passing along the credential subject
  as :body-params. If the signature is not valid, throws an invalid signature error. If
  the request body is not a credential, nothing is done."
  [handler]
  (fn [{:keys [body-params] :as req}]
    (log/trace "unwrap-credential body-params:" body-params)
    (if (cred/signed? body-params)
      (let [verified (<!! (cred/verify body-params))]
        (if-let [subject (:subject verified)]
          (let [did (:did verified)]
            (log/debug "Unwrapped credential with did:" did)
            (-> req
                (assoc :body-params subject
                       :credential/did did
                       :raw-txn body-params)
                handler))
          (throw (ex-info "Invalid credential"
                          {:response {:status 400
                                      :body   {:error "Invalid credential"}}}))))
      (do (log/trace "Request was not signed:" body-params)
          (handler req)))))

(def root-only-routes
  #{"/fluree/create"})

(defn wrap-closed-mode
  [root-identities closed-mode]
  (fn [handler]
    (fn [{:keys [body-params credential/did uri] :as req}]
      (if closed-mode
        (let [trusted-user (contains? root-identities did)]
          (cond (nil? did)
                (throw (ex-info "Authentication error: signed request required."
                                {:response {:status 400 :body {:error "Missing credential."}}}))

                (and (contains? root-only-routes uri)
                     (not trusted-user))
                (throw (ex-info "Authentication error: untrusted credential."
                                {:response {:status 403 :body {:error "Untrusted credential."}}}))

                :else
                (let [body-params* (cond-> body-params
                                     ;; don't allow escalation of priveledge
                                     (not trusted-user) (update :opts dissoc :did :role))
                      req* (assoc req :server/closed-mode closed-mode :body-params body-params*)]
                  (handler req*))))
        (handler req)))))

(defn set-track-response-header
  [track-opt handler]
  (fn [req]
    (let [resp (handler req)]
      (if-let [header-val (-> resp :body (get track-opt))]
        (let [opt-name (str "x-fdb-" (name track-opt))]
          (-> resp
              (assoc-in [:headers opt-name] (str header-val))
              (update :body dissoc track-opt)))
        resp))))

(def wrap-set-fuel-response-header
  (partial set-track-response-header :fuel))

(def wrap-set-policy-response-header
  (partial set-track-response-header :policy))

(def wrap-set-time-response-header
  (partial set-track-response-header :time))

(def fluree-request-header-keys
  ["fluree-track-meta" "fluree-track-fuel" "fluree-track-policy" "fluree-ledger"
   "fluree-max-fuel" "fluree-identity" "fluree-policy-identity" "fluree-policy"
   "fluree-policy-class" "fluree-policy-values" "fluree-format" "fluree-output"])

(defn parse-boolean-header
  [header name]
  (case (str/lower-case header)
    "false" false
    "true"  true
    (throw (ex-info (format "Invalid Fluree-%s header: must be boolean." name)
                    {:status 400, :error :server/invalid-header}))))

(def request-header-prefix-count
  (count "fluree-"))

(defn wrap-request-header-opts
  "Extract options from headers, parse and validate them where necessary, and
  attach to request. Opts set in the header override those specified within the
  transaction or query."
  [handler]
  (fn [{:keys [headers credential/did] :as req}]
    (let [{:keys [track-meta track-fuel track-policy max-fuel
                  identity policy-identity policy policy-class policy-values
                  format output ledger]}
          (-> headers
              (select-keys fluree-request-header-keys)
              (update-keys (fn [k] (keyword (subs k request-header-prefix-count)))))

          max-fuel     (when max-fuel
                         (try (Integer/parseInt max-fuel)
                              (catch Exception e
                                (throw (ex-info "Invalid Fluree-Max-Fuel header: must be integer."
                                                {:status 400, :error :server/invalid-header}
                                                e)))))
          track-meta   (some-> track-meta (parse-boolean-header "track-meta"))
          track-policy (some-> track-policy (parse-boolean-header "track-policy"))
          track-fuel   (some-> track-fuel (parse-boolean-header "track-fuel"))
          track-time   true
          track-file   true ; File tracking is required for consensus components
          meta         (if track-meta
                         (if (and track-time track-fuel track-policy track-file)
                           true
                           (cond-> {}
                             (not (false? track-time))   (assoc :time true)
                             (not (false? track-fuel))   (assoc :fuel true)
                             (not (false? track-policy)) (assoc :policy true)
                             (not (false? track-file))   (assoc :file true)
                             true                        not-empty))
                         (cond-> {}
                           track-time   (assoc :time true)
                           track-fuel   (assoc :fuel true)
                           track-policy (assoc :policy true)
                           track-file   (assoc :file true)
                           true         not-empty))
          ;; Accept header takes precedence over other ways of specifying query output
          output       (cond (-> headers (get "accept") (= "application/sparql-results+json"))
                             :sparql

                             (= output "sparql") :sparql
                             (= output "fql")    :fql
                             :else               :fql)
          ;; Content-Type header takes precedence over other ways of specifying query format
          format (cond (-> headers (get "content-type") (= "application/sparql-query"))
                       :sparql
                       (-> headers (get "content-type") (= "application/sparql-update"))
                       :sparql

                       (= format "sparql") :sparql
                       (= output "fql")    :fql
                       :else               :fql)

          policy        (when policy
                          (try (json/parse policy false)
                               (catch Exception _
                                 (throw (ex-info "Invalid Fluree-Policy header: must be JSON."
                                                 {:status 400})))))
          policy-values (when policy-values
                          (try (let [pv (json/parse policy-values false)]
                                 (if (sequential? pv)
                                   pv
                                   (throw (ex-info "Invalid Fluree-Policy-Values header, it must be a valid values binding: [[\"?varA\" \"?varB\"] [[<a1> <b1>] [<a2> <b2>] ...]]"
                                                   {:status 400}))))
                               (catch Exception _
                                 (throw (ex-info "Invalid Fluree-Policy-Values header: must be JSON."
                                                 {:status 400})))))

          opts (cond-> {}
                 meta          (assoc :meta meta)
                 max-fuel      (assoc :max-fuel max-fuel)
                 format        (assoc :format format)
                 output        (assoc :output output)
                 ledger        (assoc :ledger ledger)
                 policy        (assoc :policy policy)
                 policy-class  (assoc :policy-class policy-class)
                 policy-values (assoc :policy-values policy-values)

                 policy-identity (assoc :identity identity)
                 ;; Fluree-Identity overrides Fluree-Policy-Identity
                 identity        (assoc :identity identity)
                 ;; credential (signed) identity overrides all else
                 did             (assoc :identity did))]
      (-> req
          (assoc :fluree/opts opts)
          handler))))

(defn sort-middleware-by-weight
  [weighted-middleware]
  (map (fn [[_ mw]] mw) (sort-by first weighted-middleware)))

(def json-format
  (mf/map->Format
   {:name    "application/json"
    :matches #"^application/(.+\+)?json$" ; match application/ld+json too
    :decoder [mfj/decoder {:decode-key-fn false}] ; leave keys as strings
    :encoder [mfj/encoder]}))

(def sparql-format
  (mf/map->Format
   {:name "application/sparql-query"
    :decoder [(fn [_]
                (reify
                  mf/Decode
                  (decode [_ data charset]
                    {::query  (String. (.readAllBytes ^InputStream data)
                                       ^String charset)
                     ::format :sparql})))]}))

(def sparql-update-format
  (mf/map->Format
   {:name "application/sparql-update"
    :decoder [(fn [_]
                (reify
                  mf/Decode
                  (decode [_ data charset]
                    (String. (.readAllBytes ^InputStream data)
                             ^String charset))))]}))

(def jwt-format
  "Turn a JWT from an InputStream into a string that will be found on :body-params in the
  request map."
  (mf/map->Format
   {:name "application/jwt"
    :decoder [(fn [_]
                (reify mf/Decode
                  (decode [_ data charset]
                    (String. (.readAllBytes ^InputStream data)
                             ^String charset))))]}))

(defn debug-middleware
  "Put this in anywhere in your middleware chain to get some insight into what's
  happening there. Logs the request and response at DEBUG level, prefixed with
  the name argument."
  ([name] (debug-middleware name [] []))
  ([name req-key-path resp-key-path]
   (fn [handler]
     (fn [req]
       (when-let [req* (when req-key-path (get-in req req-key-path))]
         (log/debug name "got request:" req*))
       (let [resp (handler req)]
         (when-let [resp* (when resp-key-path (get-in resp resp-key-path))]
           (log/debug name "got response:" resp*))
         resp)))))

(defn compose-app-middleware
  [{:keys [connection consensus watcher subscriptions root-identities closed-mode]
    :as   _config}]
  (let [exception-middleware (exception/create-exception-middleware
                              (merge
                               exception/default-handlers
                               {::exception/default
                                (partial exception/wrap-log-to-console
                                         exception/http-response-handler)}))]
    ;; Exception middleware should always be first AND last.
    ;; The last (highest sort order) one ensures that middleware that comes
    ;; after it will not be skipped on response if handler code throws an
    ;; exception b/c this it catches them and turns them into responses.
    ;; The first (lowest sort order) one ensures that exceptions thrown by
    ;; other middleware are caught and turned into appropriate responses.
    ;; Seems kind of clunky. Maybe there's a better way? - WSM 2023-04-28
    (sort-middleware-by-weight [[1 exception-middleware]
                                [10 wrap-cors]
                                [10 (partial wrap-assoc-system connection consensus
                                             watcher subscriptions)]
                                [50 unwrap-credential]
                                [200 coercion/coerce-exceptions-middleware]
                                [300 coercion/coerce-response-middleware]
                                [400 coercion/coerce-request-middleware]
                                [500 wrap-request-header-opts]
                                [505 wrap-set-fuel-response-header]
                                [510 wrap-set-policy-response-header]
                                [515 wrap-set-time-response-header]
                                [600 (wrap-closed-mode root-identities closed-mode)]
                                [1000 exception-middleware]])))

(def fluree-create-routes
  ["/create"
   {:post {:summary    "Endpoint for creating new ledgers"
           :parameters {:body CreateRequestBody}
           :responses  {201 {:body CreateResponseBody}
                        400 {:body ErrorResponse}
                        409 {:body ErrorResponse}
                        500 {:body ErrorResponse}}
           :handler    #'create/default}}])

(def fluree-transact-routes
  ["/transact"
   {:post {:summary    "Endpoint for submitting transactions"
           :parameters {:body TransactRequestBody}
           :responses  {200 {:body TransactResponseBody}
                        400 {:body ErrorResponse}
                        409 {:body ErrorResponse}
                        500 {:body ErrorResponse}}
           :handler    #'srv-tx/default}}])

(def fluree-query-routes
  ["/query"
   {:get  query-endpoint
    :post query-endpoint}])

(def fluree-history-routes
  ["/history"
   {:get  history-endpoint
    :post history-endpoint}])

(def fluree-remote-routes
  ["/remote"
   ["/latestCommit"
    {:post {:summary    "Read latest commit for a ledger"
            :parameters {:body LatestCommitRequestBody}
            :handler    #'remote/latest-commit}}]
   ["/resource"
    {:post {:summary    "Read resource from address"
            :parameters {:body AddressRequestBody}
            :handler    #'remote/read-resource-address}}]
   ["/addresses"
    {:post {:summary    "Retrieve ledger address from alias"
            :parameters {:body AliasRequestBody}
            :handler    #'remote/published-ledger-addresses}}]])

(def fluree-subscription-routes
  ["/subscribe"
   {:get {:summary    "Subscribe to ledger updates"
          :parameters {:body SubscriptionRequestBody}
          :handler    #'subscription/default}}])

(def default-fluree-route-map
  {:create       fluree-create-routes
   :transact     fluree-transact-routes
   :query        fluree-query-routes
   :history      fluree-history-routes
   :remote       fluree-remote-routes
   :subscription fluree-subscription-routes})

(defn combine-fluree-routes
  [fluree-route-map]
  (->> fluree-route-map
       vals
       (into ["/fluree"])))

(def default-fluree-routes
  (combine-fluree-routes default-fluree-route-map))

(def fallback-handler
  (let [swagger-ui-handler (swagger-ui/create-swagger-ui-handler
                            {:path   "/"
                             :config {:validatorUrl     nil
                                      :operationsSorter "alpha"}})
        default-handler    (ring/create-default-handler)]
    (ring/routes swagger-ui-handler default-handler)))

(def swagger-routes
  ["/swagger.json"
   {:get {:no-doc  true
          :swagger {:info {:title "Fluree HTTP API"}}
          :handler (swagger/create-swagger-handler)}}])

(defn app-router
  [& routes]
  (let [all-routes (into [swagger-routes] routes)
        coercer    (reitit.coercion.malli/create {:strip-extra-keys false})
        formatter  (muuntaja/create
                    (-> muuntaja/default-options
                        (assoc-in [:formats "application/json"] json-format)
                        (assoc-in [:formats "application/sparql-query"] sparql-format)
                        (assoc-in [:formats "application/sparql-update"] sparql-update-format)
                        (assoc-in [:formats "application/jwt"] jwt-format)))
        middleware [swagger/swagger-feature
                    muuntaja-mw/format-negotiate-middleware
                    muuntaja-mw/format-response-middleware
                    muuntaja-mw/format-request-middleware]]
    (ring/router all-routes {:data {:coercion   coercer
                                    :muuntaja   formatter
                                    :middleware middleware}})))

(defn app
  ([config]
   (app config []))
  ([config custom-routes]
   (app config custom-routes default-fluree-routes))
  ([config custom-routes fluree-routes]
   (let [app-middleware (compose-app-middleware config)
         app-routes     (cond-> ["" {:middleware app-middleware} fluree-routes]
                          (seq custom-routes) (conj custom-routes))
         router         (app-router app-routes)]
     (ring/ring-handler router fallback-handler))))
