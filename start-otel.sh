#! /bin/bash -e

# https://opentelemetry.io/docs/zero-code/java/agent/getting-started/
# https://opentelemetry.io/docs/languages/sdk-configuration/general
# https://opentelemetry.io/docs/languages/sdk-configuration/otlp-exporter/

if [ ! -f opentelemetry-javaagent.jar ]; then
    wget https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/download/v2.13.1/opentelemetry-javaagent.jar
fi

export JAVA_TOOL_OPTIONS="-javaagent:opentelemetry-javaagent.jar"
export OTEL_SERVICE_NAME="fluree-server"
export OTEL_RESOURCE_ATTRIBUTES=service.namespace=fluree

export OTEL_TRACES_SAMPLER="always_on"
# export OTEL_EXPORTER_OTLP_ENDPOINT="http://localhost:4318"
export OTEL_INSTRUMENTATION_LOGBACK_APPENDER_EXPERIMENTAL_CAPTURE_MDC_ATTRIBUTES="*"
export OTEL_RESOURCE_PROVIDERS_AWS_ENABLED="true"

export JSON_LOGGING=true

clojure -X:deps prep
clojure -X:run-dev
