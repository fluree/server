(ns fluree.server.config.validation
  (:require [malli.core :as m]
            [malli.transform :as transform]))

(def registry
  (merge
   (m/predicate-schemas)
   (m/class-schemas)
   (m/comparator-schemas)
   (m/type-schemas)
   (m/sequence-schemas)
   (m/base-schemas)
   {::path string?
    ::server-address string?
    ::connection-storage-method [:enum
                                 :ipfs :file :memory :s3 :remote]
    ::indexing-options [:map
                        [:reindex-min-bytes {:optional true} pos-int?]
                        [:reindex-max-bytes {:optional true} pos-int?]
                        [:max-old-indexes {:optional true} nat-int?]]
    ::connection-defaults [:map
                           [:index {:optional true} ::indexing-options]
                           [:did {:optional true} :string]]
    ::file-connection [:map]
    ::memory-connection [:map]
    ::ipfs-connection [:map [:ipfs-server ::server-address]]
    ::remote-connection [:map [:remote-servers [:sequential ::server-address]]]
    ::s3-connection [:map
                     [:s3-endpoint :string]
                     [:s3-bucket :string]
                     [:s3-prefix :string]]
    ::connection [:and
                  [:map
                   [:storage-method ::connection-storage-method]
                   [:parallelism {:optional true} pos-int?]
                   [:cache-max-mb {:optional true} pos-int?]
                   [:defaults {:optional true} ::connection-defaults]]
                  [:multi {:dispatch :storage-method}
                   [:file ::file-connection]
                   [:memory ::memory-connection]
                   [:ipfs ::ipfs-connection]
                   [:remote ::remote-connection]
                   [:s3 ::s3-connection]]]
    ::server-config [:map
                     [:storage-path {:optional true} ::path]]
    ::consensus-protocol [:enum
                          :raft :standalone]
    ::raft [:map
            [:log-history {:optional true} pos-int?]
            [:entries-max {:optional true} pos-int?]
            [:catch-up-rounds {:optional true} pos-int?]
            [:servers [:sequential ::server-address]]
            [:this-server ::server-address]
            [:log-directory {:optional true} ::path]]
    ::standalone [:map [:max-pending-txns {:optional true} pos-int?]]
    ::consensus [:and
                 [:map [:protocol ::consensus-protocol]]
                 [:multi {:dispatch :protocol}
                  [:raft ::raft]
                  [:standalone ::standalone]]]
    ::http-server [:enum
                   :jetty]
    ::http-port pos-int?
    ::max-txn-wait-ms pos-int?
    ::jetty [:map [:server ::http-server]]
    ::http [:and
            [:map
             [:server ::http-server]
             [:port ::http-port]
             [:max-txn-wait-ms {:optional true} ::max-txn-wait-ms]]
            [:multi {:dispatch :server}
             [:jetty ::jetty]]]
    ::config [:map {:closed true}
              [:server ::server-config]
              [:connection ::connection]
              [:consensus ::consensus]
              [:http ::http]]

    ::sid-migration [:map
                     [:ledgers {:doc "Collection of ledger aliases to migrate."} [:sequential :string]]
                     [:force {:optional true :doc "If true, run the migration regardless of whether the ledger has already been migrated."}
                      :boolean]]

    ::sid-migration-config [:map {:closed true}
                            [:server ::server-config]
                            [:connection ::connection]
                            [:sid-migration ::sid-migration]]}))

(def coerce
  (m/coercer [:or ::config ::sid-migration-config] transform/string-transformer {:registry registry}))
