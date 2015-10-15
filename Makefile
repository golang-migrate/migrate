IMAGE=mattes/migrate
DCR=docker-compose run --rm
.PHONY: clean test build release docker-build docker-push run

all: release

clean:
	rm -f migrate

test:
	go get -t -v
	$(DCR) go-test

build:
	go get -v -t
	$(DCR) go-build

release: test build docker-build docker-push

docker-build:
	docker build --rm -t $(IMAGE) .

docker-push:
	docker push $(IMAGE)
