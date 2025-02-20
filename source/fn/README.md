# go function
```go
package main

import (
	"errors"
	"log"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/source"
	"github.com/golang-migrate/migrate/v4/source/fn"
)

func main() {
	migrations := map[string]*fn.Migration{
		"1_test": {
			Up: source.ExecutorFunc(func(i interface{}) error {
				return nil
			}),
			Down: source.ExecutorFunc(func(i interface{}) error {
				return nil
			}),
		},
	}

	d, err := fn.WithInstance(migrations)
	if err != nil {
		log.Fatalln(err)
	}
	m, err := migrate.NewWithSourceInstance("func", d, "database://foobar")
	if err != nil {
		log.Fatalln(err)
	}

	if err := m.Up(); errors.Is(err, migrate.ErrNoChange) {
		log.Println(err)
	} else if err != nil {
		log.Fatalln(err)
	}
}
```
