(ns fluree.server.otel
  "OpenTelemetry uses ThreadLocal storage to propagate trace state.
   This typically requires wrapping your tasks or executors so that this state
   is propagated to threads when desired.
   See https://github.com/open-telemetry/opentelemetry-java/blob/main/context/src/main/java/io/opentelemetry/context/Context.java
   core.async does not expose the thread pool direclty so we need to monkey patch it.

   core.async will propagate clojure var bindings, so alternatively it might be possible
   to implement a ContextStorageProvider that stores state in a var instead of thread locals.
   I spent a bit of time trying to figure this out but it seems quite a bit more involved than
   patching core.aysnc"
  (:require [clojure.core.async :as async]
            [clojure.core.async.impl.dispatch :as dispatch])
  (:import [io.opentelemetry.context Context]
           [java.util.concurrent Executor]))

(defonce original-run dispatch/run)
(defn wrapped-runable ^Runnable [^Runnable r]
  (.wrap (Context/current) r))
(defn patched-run
  "Runs Runnable r with OpenTelemetry context propagation."
  [^Runnable r]
  (original-run (wrapped-runable r)))

(alter-var-root #'dispatch/run (constantly patched-run))

(defonce ^Executor original-thread-macro-executor @#'async/thread-macro-executor)

(alter-var-root #'async/thread-macro-executor (constantly (Context/taskWrapping original-thread-macro-executor)))