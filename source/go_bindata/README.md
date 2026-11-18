# go_bindata

## Usage

### Read bindata with NewWithSourceInstance

```shell
go get -u github.com/jteeuwen/go-bindata/...
cd examples/migrations && go-bindata -pkg migrations .
```

```go
import (
  "github.com/golang-migrate/migrate/v4"
  "github.com/golang-migrate/migrate/v4/source/go_bindata"
  "github.com/golang-migrate/migrate/v4/source/go_bindata/examples/migrations"
)

func main() {
  // wrap assets into Resource
  s := bindata.Resource(migrations.AssetNames(),
    func(name string) ([]byte, error) {
      return migrations.Asset(name)
    })
    
  d, err := bindata.WithInstance(s)
  m, err := migrate.NewWithSourceInstance("go-bindata", d, "database://foobar")
  m.Up() // run your migrations and handle the errors above of course
}
```

### Read bindata with directories in filename

The default [source.Regex](https://github.com/golang-migrate/migrate/blob/master/source/parse.go#L22C1-L23C1) used in the above example assumes that the go-bindata filenames were generated from the same directory that the files exist in, if your go-bindata is in run via Makefile targets, or other automated setups or just outside of the current directory, the default will fail. To enable this, you must overwrite the regex to allow for directories.

```shell
go get -u github.com/jteeuwen/go-bindata/...
go-bindata -pkg migrations ./examples/migrations
```

```go
import (
  "regexp"

  "github.com/golang-migrate/migrate/v4"
  "github.com/golang-migrate/migrate/v4/source"
  "github.com/golang-migrate/migrate/v4/source/go_bindata"
  "github.com/golang-migrate/migrate/v4/source/go_bindata/examples/migrations"
)

func main() {
  // Overwrite the Regex with a non-capturing 0-or-more directories (this doesn't cover directories with numbers in their names)
  source.Regex = regexp.MustCompile(`^[a-zA-Z\-_\/]*([0-9]+)_(.*)\.(down|up)\.(.*)$`)

  // wrap assets into Resource
  s := bindata.Resource(migrations.AssetNames(),
    func(name string) ([]byte, error) {
      return migrations.Asset(name)
    })
    
  d, err := bindata.WithInstance(s)
  m, err := migrate.NewWithSourceInstance("go-bindata", d, "database://foobar")
  m.Up() // run your migrations and handle the errors above of course
}
```

### Read bindata with URL (todo)

This will restore the assets in a tmp directory and then
proxy to source/file. go-bindata must be in your `$PATH`.

```
migrate -source go-bindata://examples/migrations/bindata.go
```


