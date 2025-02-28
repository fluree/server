FROM --platform=$BUILDPLATFORM clojure:temurin-17-tools-deps-1.11.1.1413-bullseye-slim AS builder

RUN mkdir -p /usr/src/fluree-server
WORKDIR /usr/src/fluree-server

COPY deps.edn ./

RUN clojure -X:deps prep && clojure -P && clojure -A:build:test -P

COPY . ./

RUN clojure -T:build uber

FROM eclipse-temurin:17-jre-jammy AS runner

# Read here why UID 10001: https://github.com/hexops/dockerfile/blob/main/README.md#do-not-use-a-uid-below-10000
ARG USER_NAME=fluree
ARG USER_UID=10001
ARG USER_GID=${USER_UID}
ENV FLUREE_HOME="/opt/fluree-server"
ENV JAVA_XMX="8g"
ENV JAVA_AGENT="-javaagent:opentelemetry-javaagent.jar"
ENV JAVA_OPTS=""
ENV OTEL_EXPORTER_OTLP_ENDPOINT=""
ENV OTEL_SERVICE_NAME="fluree-server"
ENV OTEL_RESOURCE_ATTRIBUTES="service.namespace=fluree"
ENV OTEL_TRACES_SAMPLER="always_on"
ENV OTEL_INSTRUMENTATION_LOGBACK_APPENDER_EXPERIMENTAL_CAPTURE_MDC_ATTRIBUTES="*"
ENV OTEL_RESOURCE_PROVIDERS_AWS_ENABLED="true"
ENV OTEL_PROPAGATORS="xray"

ENV CONSOLE_APPENDER="CONSOLE_JSON"
ENV ROOT_LOG_LEVEL="WARN"
ENV FLUREE_LOG_LEVEL="INFO"

WORKDIR ${FLUREE_HOME}

# Creates a user with $UID and $GID=$UID
RUN groupadd --gid ${USER_GID} ${USER_NAME} && \
    useradd -m -s /bin/bash \
    -d ${FLUREE_HOME} \
    -u ${USER_UID} \
    -g ${USER_GID} ${USER_NAME} &&\
    mkdir -p ${FLUREE_HOME}/data &&\
    chown -R ${USER_NAME}:${USER_NAME} ${FLUREE_HOME}/data

USER ${USER_NAME}

COPY --from=builder --chown=${USER_NAME}:${USER_NAME} /usr/src/fluree-server/target/server-*.jar ./server.jar
COPY --chown=${USER_NAME}:${USER_NAME} docker-entrypoint.sh .
ADD --chown=${USER_NAME}:${USER_NAME} https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/download/v2.13.1/opentelemetry-javaagent.jar .

EXPOSE 8090
EXPOSE 58090

VOLUME ./data

# to run a clojure repl instead to inspect and validate configuration
# docker run -it  --entrypoint "java" fluree/server:latest -cp server.jar clojure.main
ENTRYPOINT [ "./docker-entrypoint.sh" ]
