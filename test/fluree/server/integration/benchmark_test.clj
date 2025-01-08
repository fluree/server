(ns fluree.server.integration.benchmark-test
  #_{:clj-kondo/ignore [:unused-referred-var]}
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing use-fixtures]]
            [fluree.server.integration.test-system
             :refer [api-post create-rand-ledger json-headers set-server-ports api-port]
             :as test-system]
            [fluree.server.system :as system]
            [jsonista.core :as json])
  (:import (java.time Instant)))

(def ^:private mb-size (* 1024 1024)) ; 1 MB in bytes

(def ^:private benchmark-results (atom []))

(defn run-benchmarks-with-config
  "Runs all benchmark tests with the specified configuration file."
  [config-file run-tests]
  (set-server-ports)
  (let [config (-> (io/resource config-file)
                   slurp
                   json/read-value)
        ; Find the API server config and update its port
        _ (println config)
        updated-config (update config "@graph"
                               (fn [nodes]
                                 (mapv (fn [node]
                                         (if (= "API" (get node "@type"))
                                           (assoc node "httpPort" @api-port)
                                           node))
                                       nodes)))
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
      (doseq [total-size [5 10 25 50]]
        (println (format "\nTotal Size: %d MB" total-size))
        (doseq [batch-size [1 5]]
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
        ; Use smaller effective batch size to avoid novelty limits
        effective-batch-size (min batch-size-mb 0.5) ; 0.5MB max per batch
        num-batches   (Math/ceil (/ total-size-mb effective-batch-size))
        batch-results (doall
                       (for [batch (range num-batches)]
                         (let [batch-data (generate-transaction-data effective-batch-size)
                               _         (println (format "Processing batch %d of %.0f" (inc batch) num-batches))
                               req      (json/write-value-as-string
                                         (assoc batch-data "ledger" ledger-name))
                               res      (api-post :transact {:body req :headers json-headers})]
                           (when-not (= 200 (:status res))
                             (throw (ex-info "Transaction failed"
                                             {:batch  batch
                                              :status (:status res)
                                              :body   (:body res)})))
                           ; Add delay between batches to allow indexing
                           (Thread/sleep 2000)
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
    (doseq [total-size [5 10 25 50]
            batch-size [1 5]]
      (when (<= batch-size total-size)
        (let [ledger-name (create-rand-ledger (format "benchmark-%s-%dMB-%dMB"
                                                      storage-type total-size batch-size))
              results     (run-benchmark ledger-name total-size batch-size)
              results-with-type (assoc results :storage-type storage-type)]
          (swap! benchmark-results conj results-with-type)
          (print-benchmark-results results)
          (is (> (:mb-per-second results) 0)))))))

(deftest ^:benchmark benchmark-file-storage-test
  (run-benchmarks-with-config
   "file-config.jsonld"
   #(benchmark-with-config "file")))

(deftest ^:benchmark benchmark-memory-storage-test
  (run-benchmarks-with-config
   "memory-config.jsonld"
   #(benchmark-with-config "memory")))

(deftest ^:benchmark print-benchmark-summary
  (testing "Print summary of all benchmark results"
    (print-summary-results)))
