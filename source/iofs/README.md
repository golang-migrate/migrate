# iofs

Driver with file system interface (`io/fs#FS`) supported from Go 1.16.

This Driver cannot be used with Go versions 1.15 and below.

## Usage

Directory embedding example

```go
package main

import (
	"embed"
	"log"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source/iofs"
)

//go:embed migrations
var fs embed.FS

func main() {
	d, err := iofs.WithInstance(fs, "migrations")
	if err != nil {
		log.Fatal(err)
	}
	m, err := migrate.NewWithSourceInstance("iofs", d, "postgres://postgres@localhost/postgres?sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	err = m.Up()
	// ...
}
```
