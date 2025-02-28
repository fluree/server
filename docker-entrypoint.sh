#! /bin/bash -e

# By default instrumentation is only enabled to add trace ids to
# structured logging if no OTEL_EXPORTER_OTLP_ENDPOINT is set
# then don't publish unless explicitly specified
if [ -z "$OTEL_EXPORTER_OTLP_ENDPOINT" ]; then
    export OTEL_TRACES_EXPORTER=${OTEL_TRACES_EXPORTER:-"none"}
    export OTEL_METRICS_EXPORTER=${OTEL_METRICS_EXPORTER:-"none"}
    export OTEL_LOGS_EXPORTER=${OTEL_LOGS_EXPORTER:-"none"}
fi

java -jar -Xmx$JAVA_XMX $JAVA_AGENT $JAVA_OPTS server.jar $*

