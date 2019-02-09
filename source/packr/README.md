# packr

## Usage



### Read bindata with NewWithSourceInstance


```go
import (
  "github.com/gobuffalo/packr"
  "github.com/golang-migrate/migrate/v4"
  packr_migrate "github.com/golang-migrate/migrate/v4/source/packr"
)

func main() {
  box := packr.NewBox("./migrations")

  d, err := packr_migrate.WithInstance(&box)
  if err != nil {
    log.Panicf("Error during create migrator driver: %v", err)
  }

  m, err := migrate.NewWithSourceInstance(packr_migrate.PackrName, d, "database://foobar")
  if err != nil {
    log.Panicf("Error during create migrator", err)
  }
  m.Up() // run your migrations and handle the errors above of course
}
```
