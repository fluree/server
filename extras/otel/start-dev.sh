#! /bin/bash -e

# https://opentelemetry.io/docs/zero-code/java/agent/getting-started/
# https://opentelemetry.io/docs/languages/sdk-configuration/general
# https://opentelemetry.io/docs/languages/sdk-configuration/otlp-exporter/

SCRIPT_DIR="$(dirname "$(realpath "$0")")"
cd $SCRIPT_DIR

if [ ! -f opentelemetry-javaagent.jar ]; then
    wget https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/download/v2.13.1/opentelemetry-javaagent.jar
fi
cd ../..

clojure -X:deps prep

export JAVA_AGENT="-javaagent:$SCRIPT_DIR/opentelemetry-javaagent.jar"
#export JOPTS="$JOPTS-Dio.opentelemetry.javaagent.slf4j.simpleLogger.defaultLogLevel=trace"
export JOPTS="$JAVA_AGENT $JOPTS"
export OTEL_SERVICE_NAME="fluree-server"
export OTEL_RESOURCE_ATTRIBUTES=service.namespace=fluree

export OTEL_TRACES_SAMPLER="always_on"
export OTEL_INSTRUMENTATION_LOGBACK_APPENDER_EXPERIMENTAL_CAPTURE_MDC_ATTRIBUTES="*"
export OTEL_RESOURCE_PROVIDERS_AWS_ENABLED="true"
# this tells the otel collector to use X-Amzn-Trace-Id request header as trace and span id
export export OTEL_PROPAGATORS=xray

export CONSOLE_APPENDER=${CONSOLE_APPENDER:-CONSOLE_TRACE_COLORS}
export ROOT_LOG_LEVEL=${ROOT_LOG_LEVEL:-INFO}
export FLUREE_LOG_LEVEL=${FLUREE_LOG_LEVEL:-INFO}

is_port_in_use() {
  local port=$1  # Take the first argument as the port number

  if command -v lsof &> /dev/null; then
    lsof -i :$port > /dev/null
  elif command -v ss &> /dev/null; then
    ss -tuln | grep ":$port" > /dev/null
  else
    echo "Error: No command found to check if ports is listening (lsof, or ss)" >&2
    return 0
  fi
}

# default endpoint is http://localhost:4318 with expectation that a sidecar
# is listening. It can still be useful to enable instrumentation locally
# to get trace ids in logs even if nothing is listening
# export OTEL_EXPORTER_OTLP_ENDPOINT="http://localhost:4318"
if [ -z "$OTEL_EXPORTER_OTLP_ENDPOINT" ] && ! is_port_in_use 4318; then
    # instrument app for structured logging, but don't actually export the data
    echo "Instrumentation is enabled but nothing is listening, exporters will be disabled"
    export OTEL_TRACES_EXPORTER="none"
    export OTEL_METRICS_EXPORTER="none"
    export OTEL_LOGS_EXPORTER="none"
fi

# when passing java opts to clojure each option needs to be prefixed with -J
# it complains if you do -J"multiple space deliminated options"
prepend_J() {
  local result=()
  for arg in "$@"; do
    result+=("-J$arg")
  done
  echo "${result[@]}"
}
JOPTS=$(prepend_J $JOPTS)

if [ "$1" == "repl" ]; then
    export PROFILES=${PROFILES:-":dev:dev/reloaded:run-dev:test"}
    if [ -n "$2" ]; then
      PROFILES="$2"
    fi
    clojure $JOPTS -Sdeps '{:deps {nrepl/nrepl {:mvn/version,"1.1.0"},cider/cider-nrepl {:mvn/version,"0.45.0"}}}' -M:$PROFILES -m nrepl.cmdline --middleware "[cider.nrepl/cider-middleware]"
else
    clojure $JOPTS -X:run-dev
fi
