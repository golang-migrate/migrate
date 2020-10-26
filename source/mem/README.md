# inmem

In memory (`inmem`) driver useful when you want to always includes your database schema migration along with your binary.


## Usage

There are 2 way to use in memory migration driver:

* Using `WithInstance` so you can use it anywhere without worrying about `key` identifier conflict. Or
* Using `RegisterMigrations` so you can migrate from anywhere in your code using `key` in URL.

Whichever the method you use, you still need to implement `mem.Migration` for all your migration schema.

### Implements `mem.Migration`

Create struct and implement all method receiver for `mem.Migration`:

```go
type DummyMigration struct {
	Ver       uint
	UpQuery   string
	DownQuery string
}

func (m DummyMigration) Version() uint { return m.Ver }

func (m DummyMigration) Up() string { return m.UpQuery }

func (m DummyMigration) Down() string { return m.DownQuery }

var _ inmem.Migration = (*DummyMigration)(nil)
```


### Using `WithInstance`
```go
import (
  "github.com/golang-migrate/migrate/v4"
  "github.com/golang-migrate/migrate/v4/source/mem"
)

func main() {
    createUserTable := &DummyMigration{
		Ver:       1,
		UpQuery:   "CREATE TABLE IF NOT EXISTS users(id bigint primary key, username varchar);",
		DownQuery: "DROP TABLE IF EXISTS users;",
	}

	driver, err := mem.WithInstance(createUserTable)
	m, err := migrate.NewWithSourceInstance("mem", driver, "database://foobar")
	m.Up() // run your migrations and handle the errors above of course
}
```

### Using `RegisterMigrations`

```go
import (
  "github.com/golang-migrate/migrate/v4"
  "github.com/golang-migrate/migrate/v4/source/inmem"
)

func main() {
    createUserTable := &DummyMigration{
		Ver:       1,
		UpQuery:   "CREATE TABLE IF NOT EXISTS users(id bigint primary key, username varchar);",
		DownQuery: "DROP TABLE IF EXISTS users;",
	}

	key := "myUniqueKey"
	err := mem.RegisterMigrations(key, createUserTable)
	m, err := migrate.New("mem://"+key, "database://foobar")
	err = m.Up() // run your migrations and handle the errors above of course
}
```

