SOURCE?=file go-bindata github
DATABASE?=postgres
VERSION?=$(shell git describe --tags 2>/dev/null)
TEST_FLAGS?=

build-cli: clean
	-mkdir ./cli/build
	cd ./cli && GOOS=linux GOARCH=amd64 go build -a -o build/migrate.linux-amd64 -ldflags="-X main.Version=$(VERSION)" -tags '$(DATABASE) $(SOURCE)' . 
	cd ./cli && GOOS=darwin GOARCH=amd64 go build -a -o build/migrate.darwin-amd64 -ldflags="-X main.Version=$(VERSION)" -tags '$(DATABASE) $(SOURCE)' . 
	cd ./cli && GOOS=windows GOARCH=amd64 go build -a -o build/migrate.windows-amd64.exe -ldflags="-X main.Version=$(VERSION)" -tags '$(DATABASE) $(SOURCE)' . 
	cd ./cli/build && find . -name 'migrate*' | xargs -I{} tar czf {}.tar.gz {}
	cd ./cli/build && shasum -a 256 * > sha256sum.txt
	cat ./cli/build/sha256sum.txt

clean:
	-rm -r ./cli/build

test-short:
	make test-with-flags --ignore-errors TEST_FLAGS='-short'

test:
	make test-with-flags TEST_FLAGS='-race -v -cover -bench=. -benchmem'

coverage:
	make test-with-flags TEST_FLAGS='-cover -short'

test-with-flags:
	@echo SOURCE: $(SOURCE) 
	@echo DATABASE: $(DATABASE)

	@go test $(TEST_FLAGS) .
	@go test $(TEST_FLAGS) ./cli/...

	@go test $(TEST_FLAGS) ./source/{$(subst $(space),$(,),$(SOURCE)),}
	@go test $(TEST_FLAGS) ./source/testing
	@go test $(TEST_FLAGS) ./source/stub

	@go test $(TEST_FLAGS) ./database/{$(subst $(space),$(,),$(DATABASE)),}
	@go test $(TEST_FLAGS) ./database/testing 
	@go test $(TEST_FLAGS) ./database/stub 
	
	# deprecated v1compat:
	@go test $(TEST_FLAGS) ./migrate/...

deps:
	-go get -v -u ./... 
	-go test -v -i ./...

.PHONY: build-cli clean test-short test coverage test-with-flags deps
SHELL=/bin/bash

# define comma and space
, := ,
space :=  
space +=  

