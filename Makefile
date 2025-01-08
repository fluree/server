SOURCES := $(shell find src)
RESOURCES := $(shell find resources)

target/server-%.jar: $(SOURCES) $(RESOURCES) prepare
	clojure -T:build uber

.PHONY: uberjar
uberjar: target/server-%.jar

.DEFAULT_GOAL := uberjar

.PHONY: prepare
prepare:
	clojure -X:deps prep

.PHONY: docker-build
docker-build:
	docker buildx build -t fluree/server:latest -t fluree/server:$(shell git rev-parse HEAD) --build-arg="PROFILE=prod" --load .

.PHONY: docker-run
docker-run:
	docker run -p 58090:8090 -v `pwd`/data:/opt/fluree-server/data fluree/server

.PHONY: docker-push
docker-push:
	docker buildx build --platform linux/amd64,linux/arm64 -t fluree/server:latest -t fluree/server:$(shell git rev-parse HEAD) --build-arg="PROFILE=prod" --push .

.PHONY: test
test: prepare
	clojure -X:test

.PHONY: benchmark
benchmark: prepare
	clojure -X:benchmark

.PHONY: pending-tests
pending-tests: prepare
	clojure -X:pending-tests

.PHONY: pt
pt: pending-tests

.PHONY: clj-kondo-lint
clj-kondo-lint: prepare
	clj-kondo --lint src:test:build.clj

.PHONY: clj-kondo-lint-ci
clj-kondo-lint-ci: prepare
	clj-kondo --lint src:test:build.clj --config .clj-kondo/ci-config.edn

.PHONY: cljfmt-check
cljfmt-check: prepare
	cljfmt check src test build.clj

.PHONY: clean
clean:
	rm -rf target
	rm -rf .cpcache
