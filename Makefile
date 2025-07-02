SOURCES := $(shell find src)
RESOURCES := $(shell find resources)

.PHONY: help
help: ## Describe available tasks
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'

target/server-%.jar: $(SOURCES) $(RESOURCES)
	clojure -T:build uber

.PHONY: uberjar
uberjar: target/server-%.jar ## Build an executable server uberjar

.DEFAULT_GOAL := uberjar

.PHONY: docker-build
docker-build: ## Build the server docker container
	docker buildx build -t fluree/server:latest -t fluree/server:$(shell git rev-parse HEAD) --build-arg="PROFILE=prod" --load .

.PHONY: docker-run
docker-run: ## Run in docker
	docker run -p 58090:8090 -v `pwd`/data:/opt/fluree-server/data fluree/server

.PHONY: docker-push
docker-push: ## Build and publish the server docker container
	docker buildx build --platform linux/amd64,linux/arm64 -t fluree/server:latest -t fluree/server:$(shell git rev-parse HEAD) --build-arg="PROFILE=prod" --push .

.PHONY: test
test: ## Run tests
	clojure -X:test

.PHONY: benchmark
benchmark: ## Benchmark performance
	clojure -X:benchmark

.PHONY: pending-tests
pending-tests: ## Run pending tests
	clojure -X:pending-tests

.PHONY: pt
pt: pending-tests

.PHONY: clj-kondo-lint
clj-kondo-lint: ## Lint Clojure code with clj-kondo
	clj-kondo --lint src:test:build.clj

.PHONY: clj-kondo-lint-ci
clj-kondo-lint-ci:
	clj-kondo --lint src:test:build.clj --config .clj-kondo/ci-config.edn

.PHONY: cljfmt-check
cljfmt-check: ## Check Clojure formatting with cljfmt
	cljfmt check src test build.clj

.PHONY: cljfmt-fix ## Fix Clojure formatting errors with cljfmt
cljfmt-fix:
	cljfmt fix src dev test build.clj

.PHONY: clean
clean: ## Remove build artifacts
	rm -rf target
	rm -rf .cpcache

.PHONY: run
run: ## Run the server without building JAR (development mode)
	clojure -M:run-dev

.PHONY: run-prod
run-prod: ## Run the server without building JAR (production mode)
	clojure -M:run-prod

.PHONY: build-and-run
build-and-run: uberjar ## Build JAR and run the server
	java -jar target/server-*.jar
