(ns fluree.server.handlers.ledger
  (:require [clojure.core.async :as async]
            [clojure.java.io :as io]
            [fluree.db.api :as fluree]
            [fluree.db.util :as util]
            [fluree.db.util.json :as json]
            [fluree.db.util.log :as log]
            [fluree.server.handler :as-alias handler]
            [fluree.server.handlers.shared :refer [defhandler deref!] :as shared])
  (:import (ring.core.protocols StreamableResponseBody)))

(defn ndjson-streaming-body
  "Converts a core.async channel of results into a Ring StreamableResponseBody
   that writes NDJSON (newline-delimited JSON) to the output stream.

   Handles:
   - Normal results: written as JSON objects
   - Exceptions: written as error objects with :error and :status keys
   - Metadata: {:_fluree-meta {:status 200, :fuel ..., :time ..., :policy ...}}
     written as final line when query tracking is enabled

   Each item is written as a JSON line followed by newline.
   The stream closes when the channel closes."
  [result-ch]
  (reify StreamableResponseBody
    (write-body-to-stream [_ _ output-stream]
      (with-open [writer (io/writer output-stream)]
        (loop []
          (when-some [result (async/<!! result-ch)]
            (cond
              ;; Exception - write error object and close stream
              (util/exception? result)
              (let [error-data (ex-data result)
                    status (or (:status error-data) 500)
                    error-obj {:error (ex-message result)
                               :status status
                               :data (dissoc error-data :status)}]
                (log/error result "Error during streaming query")
                (.write writer (json/stringify error-obj))
                (.write writer "\n")
                (.flush writer))

              ;; Metadata (final message from fluree/db) - write as-is
              (:_fluree-meta result)
              (do
                (.write writer (json/stringify result))
                (.write writer "\n")
                (.flush writer))

              ;; Normal result - write and continue
              :else
              (do
                (.write writer (json/stringify result))
                (.write writer "\n")
                (.flush writer)
                (recur)))))))))

(defhandler query
  [{:keys [fluree/conn fluree/opts headers] {:keys [body]} :parameters :as _req}]
  (let [query (or (::handler/query body) body)
        accept (get headers "accept")
        streaming? (or (= accept "application/x-ndjson")
                       (= accept "application/ndjson"))]

    (if streaming?
      ;; Streaming response - use NDJSON
      (do
        (log/debug "Executing streaming query:" query "with opts:" opts)
        (let [result-ch (fluree/query-connection-stream conn query opts)]
          ;; Return 206 Partial Content to avoid response coercion middleware
          ;; (only 200 responses are coerced against QueryResponse schema)
          {:status 206
           :headers {"content-type" "application/x-ndjson"
                     "cache-control" "no-cache"
                     "x-content-type-options" "nosniff"}
           :body (ndjson-streaming-body result-ch)}))

      ;; Buffered response (existing behavior)
      (let [{:keys [status result] :as query-response}
            (deref! (fluree/query-connection conn query opts))]
        (log/debug "query handler received query:" query opts)
        (shared/with-tracking-headers {:status status, :body result}
          query-response)))))

(defhandler history
  [{:keys [fluree/conn fluree/opts] {{ledger :from :as query} :body} :parameters :as _req}]
  (let [query*  (dissoc query :from)
        result  (deref! (fluree/history conn ledger query* opts))]
    (log/debug "history handler received query:" query opts "result:" result)
    ;; fluree/history may return either raw result or wrapped in {:status :result}
    (if (and (map? result) (:status result) (:result result))
      {:status (:status result), :body (:result result)}
      {:status 200, :body result})))
