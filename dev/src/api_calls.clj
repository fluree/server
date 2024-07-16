(ns api-calls
  (:require [fluree.db.api :as fluree]))


(comment
  (def conn @(fluree/connect {:method       :file,
                              :parallelism  2,
                              :storage-path "data/srv3/ledger",
                              :defaults     {:context {:id   "@id",
                                                       :type "@type",
                                                       :ex   "http://example.com/"}
                                             :indexer {:reindex-min-bytes 9
                                                       :reindex-max-bytes 10000000}}}))

  (def ledger @(fluree/load conn "my/test1"))

  )
