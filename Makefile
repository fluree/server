SOURCES := $(shell find src)
RESOURCES := $(shell find resources)

target/server-%.jar: $(SOURCES) $(RESOURCES)
	clojure -T:build uber

uberjar: target/server-%.jar

docker-build:
	docker buildx build --platform linux/amd64,linux/arm64 -t fluree/server:latest -t fluree/server:$(shell git rev-parse HEAD) --build-arg="PROFILE=prod" .

docker-run:
	docker run -p 58090:8090 -v `pwd`/data:/opt/fluree-server/data fluree/server

docker-push:
	docker buildx build --platform linux/amd64,linux/arm64 -t fluree/server:latest -t fluree/server:$(shell git rev-parse HEAD) --build-arg="PROFILE=prod" --push .

.PHONY: test
test:
	clojure -X:test

.PHONY: eastwood
eastwood:
	clojure -M:dev:test:eastwood

.PHONY: clean
clean:
	rm -rf target
	rm -rf .cpcache
