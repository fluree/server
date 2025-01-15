(ns benchmark
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.test :refer [is testing]]
            [fluree.server.integration.test-system
             :refer [api-post create-rand-ledger json-headers set-server-ports api-port]
             :as test-system]
            [fluree.server.system :as system]
            [jsonista.core :as json])
  (:import (java.time Instant)))

(def ^:private mb-size (* 1024 1024)) ; 1 MB in bytes

(def ^:private test-data-dir "dev/data/benchmark")

(def ^:private benchmark-results (atom []))

(defn- clear-test-data-dir
  "Deletes all contents of the test-data-dir, including files and directories."
  []
  (let [dir (io/file test-data-dir)
        abs-dir (.getAbsoluteFile dir)] ;; Resolve to absolute path
    (println "Clearing test-data-dir contents from " abs-dir)
    (if (.exists abs-dir)
      (do
        (doseq [file (reverse (file-seq abs-dir))] ;; Reverse to delete children first
          (io/delete-file file true))
        (io/delete-file abs-dir true)
        (println "Cleared test-data-dir contents."))
      (println "Directory does not exist:" (.getAbsolutePath dir)))))

(defn run-benchmarks-with-config
  "Runs all benchmark tests with the specified configuration file."
  [config-file run-tests]
  (set-server-ports)
  (let [config (-> (io/resource config-file)
                   slurp
                   json/read-value)
        ; Find the API server config and update its port
        updated-port-config (update config "@graph"
                                    (fn [nodes]
                                      (mapv (fn [node]
                                              (if (= "API" (get node "@type"))
                                                (assoc node "httpPort" @api-port)
                                                node))
                                            nodes)))
        updated-config (if (= config-file "file-config.jsonld")
                         (update updated-port-config "@graph"
                                 (fn [nodes]
                                   (mapv (fn [node]
                                           (if (= "Storage" (get node "@type"))
                                             (assoc node "filePath" test-data-dir)
                                             node))
                                         nodes)))
                         updated-port-config)
        _ (println updated-config)
        server (system/start-config updated-config)]
    (try
      (run-tests)
      (finally
        (system/stop server)))))

(defn print-summary-results
  "Prints a summary of all benchmark results."
  []
  (let [results @benchmark-results]
    (println "\n=== Benchmark Summary ===")
    (doseq [storage-type ["file" "memory"]]
      (println (format "\n%s Storage Results:" (str/upper-case storage-type)))
      (doseq [total-size [1 5 10]]
        (println (format "\nTotal Size: %d MB" total-size))
        (doseq [batch-size [1 5 10]]
          (when (<= batch-size total-size)
            (let [matching-results (filter #(and (= (:storage-type %) storage-type)
                                                 (= (:total-size-mb %) total-size)
                                                 (= (:batch-size-mb %) batch-size))
                                           results)]
              (when (seq matching-results)
                (let [avg-mb-per-sec (/ (reduce + (map :mb-per-second matching-results))
                                        (count matching-results))]
                  (println (format "  Batch Size %d MB: %.2f MB/second"
                                   batch-size
                                   avg-mb-per-sec)))))))))))

(defn- generate-transaction-data
  "Generates a transaction of approximately size-mb megabytes.
   Returns a map in the expected JSON-LD transaction format.
   Creates a graph of related entries to test relationship indexing."
  [size-mb]
  (let [base-str    "This is test data entry number "
        ; Calculate how many entries we need to reach target size
        ; Account for additional size from relationship data
        entry-size  (+ (count base-str) 100) ; increased size estimate for relationship overhead
        num-entries (int (/ (* size-mb mb-size) entry-size))
        entries     (for [i (range num-entries)]
                      (let [next-id (if (= i (dec num-entries))
                                      (str "ex:benchmark-0") ; link back to first for last entry
                                      (str "ex:benchmark-" (inc i)))]
                        {"id"          (str "ex:benchmark-" i)
                         "type"        "ex:BenchmarkEntry"
                         "ex:number"   i
                         "ex:data"     (str base-str i)
                         "ex:created"  (str (Instant/now))
                         "ex:relatedTo" {"id" next-id}
                        ; Add some additional relationships to create a denser graph
                         "ex:relatedToRandom" {"id" (str "ex:benchmark-"
                                                         (rand-int num-entries))}}))]
    {"@context" test-system/default-context
     "insert"   (vec entries)}))

(defn- run-benchmark
  "Runs a benchmark test with the given parameters:
   total-size-mb: Total size of data to transact
   batch-size-mb: Size of each individual transaction
   Returns a map of performance metrics."
  [ledger-name total-size-mb batch-size-mb]
  (let [start-time    (System/nanoTime)
        num-batches   (Math/ceil (/ total-size-mb batch-size-mb))
        batch-results (doall
                       (for [batch (range num-batches)]
                         (let [batch-data (generate-transaction-data batch-size-mb)
                               _         (println (format "Processing batch %d of %.0f" (inc batch) num-batches))
                               req      (json/write-value-as-string
                                         (assoc batch-data "ledger" ledger-name))
                               res      (api-post :transact {:body req :headers json-headers})]
                           (when-not (= 200 (:status res))
                             (throw (ex-info "Transaction failed"
                                             {:batch  batch
                                              :status (:status res)
                                              :body   (:body res)})))
                           {:batch-num    batch
                            :status       (:status res)
                            :time-taken   (/ (- (System/nanoTime) start-time) 1e9)})))
        end-time      (System/nanoTime)
        total-time    (/ (- end-time start-time) 1e9)]
    {:total-size-mb    total-size-mb
     :batch-size-mb    batch-size-mb
     :total-time-secs  total-time
     :mb-per-second    (/ total-size-mb total-time)
     :batch-results    batch-results}))

(defn- print-benchmark-results
  "Pretty prints the benchmark results"
  [{:keys [total-size-mb batch-size-mb total-time-secs mb-per-second]}]
  (println "\nBenchmark Results:")
  (println (format "Total Size: %d MB" total-size-mb))
  (println (format "Batch Size: %d MB" batch-size-mb))
  (println (format "Total Time: %.2f seconds" total-time-secs))
  (println (format "Performance: %.2f MB/second" mb-per-second)))

(defn benchmark-with-config
  [storage-type]
  (testing (format "Benchmark performance with %s storage configuration" storage-type)
    (doseq [total-size [1 5 10]
            batch-size [1 5 10]]
      (when (<= batch-size total-size)
        (let [ledger-name (create-rand-ledger (format "benchmark-%s-%dMB-%dMB"
                                                      storage-type total-size batch-size))
              results     (run-benchmark ledger-name total-size batch-size)
              results-with-type (assoc results :storage-type storage-type)]
          (swap! benchmark-results conj results-with-type)
          (print-benchmark-results results)
          (is (> (:mb-per-second results) 0)))))))

(defn run-benchmark-suite [_]
  (clear-test-data-dir)
  (run-benchmarks-with-config
   "file-config.jsonld"
   #(benchmark-with-config "file"))
  (run-benchmarks-with-config
   "memory-config.jsonld"
   #(benchmark-with-config "memory"))
  (print-summary-results))
