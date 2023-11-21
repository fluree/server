FROM --platform=$BUILDPLATFORM clojure:temurin-17-tools-deps-1.11.1.1413-bullseye-slim AS builder

RUN mkdir -p /usr/src/fluree-server
WORKDIR /usr/src/fluree-server

COPY deps.edn ./

RUN clojure -P && clojure -A:build:test -P

COPY . ./

RUN clojure -T:build uber

FROM eclipse-temurin:17-jre-jammy AS runner

RUN mkdir -p /opt/fluree-server
WORKDIR /opt/fluree-server

COPY --from=builder /usr/src/fluree-server/target/server-*.jar ./server.jar

EXPOSE 8090
EXPOSE 58090

VOLUME ./data

ENTRYPOINT ["java", "-jar", "server.jar"]
CMD ["docker"]
