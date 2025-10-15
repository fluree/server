(ns fluree.server.fedq-demo
  (:require [clojure.java.io :as io]
            [fluree.db.api :as fluree]
            [fluree.db.util.json :as json]
            [fluree.db.util.log :as log]))

(def ledger-alias "test-federated-query")

(defn load-data
  [conn]
  (let [gentox  (slurp (io/resource "GenTox_DB_Ontology.jsonld"))
        ames    (slurp (io/resource "tblincubation_ames.jsonld"))
        testorg (slurp (io/resource "lst_testorganism.jsonld"))
        ecoli   (slurp (io/resource "eColi_expansion.jsonld"))

        db0 @(fluree/create conn ledger-alias {:indexing {:reindex-min-bytes 25000000
                                                          :reindex-max-bytes 150000000}})
        db1 @(fluree/insert db0 (json/parse gentox false))
        db2 @(fluree/insert db1 (json/parse ames false))
        db3 @(fluree/insert db2 (json/parse testorg false))
        db4 @(fluree/insert db3 (json/parse ecoli false))
        db5 @(fluree/commit! conn db4)]
    (log/info "Finished transacting demo data:" (-> db5 :stats :flakes) "flakes")
    :success))
