SOURCE ?= file go-bindata github
DATABASE ?= postgres
VERSION ?= $(shell git describe --tags 2>/dev/null)
TEST_FLAGS ?=


build-cli: clean
	-mkdir ./cli/build
	cd ./cli && GOOS=linux GOARCH=amd64 go build -a -o build/migrate.linux-amd64 -ldflags='-X main.Version=$(VERSION)' -tags '$(DATABASE) $(SOURCE)' . 
	cd ./cli && GOOS=darwin GOARCH=amd64 go build -a -o build/migrate.darwin-amd64 -ldflags='-X main.Version=$(VERSION)' -tags '$(DATABASE) $(SOURCE)' . 
	cd ./cli && GOOS=windows GOARCH=amd64 go build -a -o build/migrate.windows-amd64.exe -ldflags='-X main.Version=$(VERSION)' -tags '$(DATABASE) $(SOURCE)' . 
	cd ./cli/build && find . -name 'migrate*' | xargs -I{} tar czf {}.tar.gz {}
	cd ./cli/build && shasum -a 256 * > sha256sum.txt
	cat ./cli/build/sha256sum.txt


clean:
	-rm -r ./cli/build


test-short:
	make test-with-flags --ignore-errors TEST_FLAGS='-short'


test:
	@-rm -r .coverage
	@mkdir .coverage
	make test-with-flags TEST_FLAGS='-v -race -covermode atomic -coverprofile .coverage/_$$(RAND).txt  -bench=. -benchmem'
	@echo 'mode: atomic' > .coverage/combined.txt
	@cat .coverage/*.txt | grep -v 'mode: atomic' >> .coverage/combined.txt


test-with-flags:
	@echo SOURCE: $(SOURCE) 
	@echo DATABASE: $(DATABASE)

	@go test $(TEST_FLAGS) .
	@go test $(TEST_FLAGS) ./cli/...
	@go test $(TEST_FLAGS) ./testing/...

	@echo -n '$(SOURCE)' | tr -s ' ' '\n' | xargs -I{} go test $(TEST_FLAGS) ./source/{}
	@go test $(TEST_FLAGS) ./source/testing
	@go test $(TEST_FLAGS) ./source/stub

	@echo -n '$(DATABASE)' | tr -s ' ' '\n' | xargs -I{} go test $(TEST_FLAGS) ./database/{}
	@go test $(TEST_FLAGS) ./database/testing 
	@go test $(TEST_FLAGS) ./database/stub 
	
	# deprecated v1compat:
	@go test ./migrate/...


html-coverage:
	go tool cover -html=.coverage/combined.txt


deps:
	-go get -v -u ./... 
	-go test -v -i ./...


.PHONY: build-cli clean test-short test test-with-flags deps html-coverage
SHELL = /bin/bash
RAND = $(shell echo $$RANDOM)

