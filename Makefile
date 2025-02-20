SOURCES := $(shell find src)
RESOURCES := $(shell find resources)

.PHONY: help ## Describe available tasks
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'

target/server-%.jar: $(SOURCES) $(RESOURCES) prepare
	clojure -T:build uber

.PHONY: uberjar
uberjar: target/server-%.jar ## Build an executable server uberjar

.DEFAULT_GOAL := uberjar

.PHONY: prepare
prepare: ## Prepare clojure dependencies
	clojure -X:deps prep

.PHONY: docker-build
docker-build: ## Build the server docker container
	docker buildx build -t fluree/server:latest -t fluree/server:$(shell git rev-parse HEAD) --build-arg="PROFILE=prod" --load .

.PHONY: docker-run
docker-run: ## Run in docker
	docker run -p 58090:8090 -v `pwd`/data:/opt/fluree-server/data fluree/server

.PHONY: docker-push
docker-push: ## Build and publish the server docker container
	docker buildx build --platform linux/amd64,linux/arm64 -t fluree/server:latest -t fluree/server:$(shell git rev-parse HEAD) --build-arg="PROFILE=prod" --push .

.PHONY: test ## Run tests
test: prepare
	clojure -X:test

.PHONY: benchmark
benchmark: prepare ## Benchmark performance
	clojure -X:benchmark

.PHONY: pending-tests ## Run pending tests
pending-tests: prepare
	clojure -X:pending-tests

.PHONY: pt
pt: pending-tests

.PHONY: clj-kondo-lint
clj-kondo-lint: prepare ## Lint Clojure code with clj-kondo
	clj-kondo --lint src:test:build.clj

.PHONY: clj-kondo-lint-ci
clj-kondo-lint-ci: prepare
	clj-kondo --lint src:test:build.clj --config .clj-kondo/ci-config.edn

.PHONY: cljfmt-check
cljfmt-check: prepare ## Check Clojure formatting with cljfmt
	cljfmt check src test build.clj

.PHONY: clean
clean: ## Remove build artifacts
	rm -rf target
	rm -rf .cpcache
