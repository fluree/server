(ns fluree.server.handlers.ledger-resource
  (:require [clojure.string :as str]
            [fluree.db.util.log :as log]
            [fluree.server.handlers.create :as create]
            [fluree.server.handlers.drop :as drop]
            [fluree.server.handlers.ledger :as ledger]
            [fluree.server.handlers.shared :refer [defhandler]]
            [fluree.server.handlers.transact :as tx]))

(set! *warn-on-reflection* true)

(def supported-operations
  "Set of supported ledger operations as path suffixes"
  #{":create" ":insert" ":upsert" ":update" ":delete"
    ":history" ":drop"})

(defn parse-ledger-operation
  "Parse a path to extract ledger name and operation.
   Walks backwards through path segments looking for a known operation.
   Returns {:ledger-name '...' :operation :insert/:upsert/etc}
   
   Examples:
   'my-ledger/:insert' -> {:ledger-name 'my-ledger' :operation :insert}
   'my/nested/ledger/:update' -> {:ledger-name 'my/nested/ledger' :operation :update}
   'my-ledger' -> {:ledger-name 'my-ledger' :operation nil}"
  [path]
  (let [path-parts (str/split path #"/")
        reversed-parts (reverse path-parts)]
    (loop [parts reversed-parts
           seen-parts []]
      (if-let [part (first parts)]
        (if (supported-operations part)
          ;; Found an operation, everything before it is the ledger name
          {:ledger-name (str/join "/" (reverse (rest parts)))
           :operation (keyword (subs part 1))}
          ;; Keep looking, accumulating parts
          (recur (rest parts) (conj seen-parts part)))
        ;; No operation found, entire path is ledger name
        {:ledger-name (str/join "/" (reverse seen-parts))
         :operation nil}))))

(defn wrap-ledger-extraction
  "Middleware to extract ledger name and operation from path.
   Adds :ledger to fluree/opts and :fluree/operation to request."
  [handler]
  (fn [req]
    (if-let [ledger-path (get-in req [:parameters :path :ledger-path])]
      (let [{:keys [ledger-name operation]} (parse-ledger-operation ledger-path)]
        (log/debug "Extracted ledger:" ledger-name "operation:" operation "from path:" ledger-path)
        (-> req
            (assoc-in [:fluree/opts :ledger] ledger-name)
            (assoc :fluree/operation operation)
            handler))
      (handler req))))

(defhandler dispatch
  [{:keys [fluree/operation] :as req}]
  (log/debug "Dispatching ledger resource operation:" operation)
  (case operation
    :create (create/default req)
    :insert (tx/insert req)
    :upsert (tx/upsert req)
    :update (tx/update req)
    :delete (tx/update req) ; delete is just an update operation
    :drop   (let [ledger-name (get-in req [:fluree/opts :ledger])]
              (drop/drop-handler (assoc-in req [:parameters :body :ledger] ledger-name)))
    :history (let [ledger-name (get-in req [:fluree/opts :ledger])]
               (ledger/history (assoc-in req [:parameters :body :from] ledger-name)))
    ;; No operation specified - default to query for GET, error for POST
    (if (= :get (:request-method req))
      (ledger/query req)
      (throw (ex-info "Operation required for POST requests to ledger resource"
                      {:status 400
                       :error :db/invalid-operation})))))