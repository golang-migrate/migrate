TESTFLAGS?=
IMAGE=mattes/migrate
DCR=docker-compose run --rm
GOTEST=go test $(TESTFLAGS) `go list  ./... | grep -v "/vendor/"`

.PHONY: clean test build release docker-build docker-push run
all: release

clean:
	rm -f migrate

fmt:
	@gofmt -s -w `go list -f {{.Dir}} ./... | grep -v "/vendor/"`

test: fmt
	$(DCR) go-test

go-test: fmt
	@$(GOTEST)

build:
	$(DCR) go-build

release: test build docker-build docker-push

docker-build:
	docker build --rm -t $(IMAGE) .

docker-push:
	docker push $(IMAGE)
