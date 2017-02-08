# go-bindata


## Usage


First create `bindata.go` 

```
go get -u github.com/jteeuwen/go-bindata/...
cd examples/migrations && go-bindata -pkg migrations .
```

Then use it in CLI:

// 

```
// TODO
// this will restore the assets in a tmp directory and then 
// proxy to source/file
// go-bindata must be in your $PATH
migrate -source go-bindata://examples/migrations/bindata.go 
```

or with library:

```
import (
  "github.com/mattes/migrate"
  "github.com/mattes/migrate/source/go-bindata"
  "github.com/mattes/migrate/source/go-bindata/examples/migrations
)

func main() {
	// wrap assets into Resource
  s := bindata.Resource(migrations.AssetNames(),
    func(name string) ([]byte, error) {
      return migrations.Asset(name)
    })

  m, err := migrate.NewWithSourceInstance("go-bindata", resource, "database://foobar")
  m.Up() // run your migrations and handle the errors above of course
}
```





