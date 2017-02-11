# Development, Testing and Contributing

  1. Make sure you have a running Docker daemon
     (Install for [MacOS](https://docs.docker.com/docker-for-mac/))
  2. Fork this repo and `git clone` somewhere to `$GOPATH/src/github.com/%you%/migrate`
  3. `make rewrite-import-paths` to update imports to your local fork
  4. Confirm tests are working: `make test-short`
  5. Write awesome code ...
  6. `make test` to run all tests against all database versions
  7. `make restore-import-paths` to restore import paths
  8. Push code and open Pull Request
 
Some more helpful commands:

  * You can specify which database/ source tests to run:  
    `make test-short SOURCE='file go-bindata' DATABASE='postgres cassandra'`
  * After `make test`, run `make html-coverage` which opens a shiny test coverage overview.  
  * Missing imports? `make deps`
  * `make build-cli` builds the CLI in directory `cli/build/`.
  * `make list-external-deps` lists all external dependencies for each package
  * `make docs && make open-docs` opens godoc in your browser, `make kill-docs` kills the godoc server.  
    Repeatedly call `make docs` to refresh the server.  
