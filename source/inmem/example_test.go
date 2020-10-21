package inmem_test

import (
	"log"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source/inmem"
)

type DummyMigration struct {
	Ver       uint
	UpQuery   string
	DownQuery string
}

func (m DummyMigration) Version() uint { return m.Ver }

func (m DummyMigration) Up() string { return m.UpQuery }

func (m DummyMigration) Down() string { return m.DownQuery }

var _ inmem.Migration = (*DummyMigration)(nil)

func ExampleWithInstance() {
	createUserTable := &DummyMigration{
		Ver:       1,
		UpQuery:   "CREATE TABLE IF NOT EXISTS users(id bigint primary key, username varchar);",
		DownQuery: "DROP TABLE IF EXISTS users;",
	}

	driver, _ := inmem.WithInstance(createUserTable)
	m, err := migrate.NewWithSourceInstance("inmem", driver, "database://foobar")
	if err != nil {
		log.Fatal(err)
		return
	}

	err = m.Up() // run your migrations and handle the errors above of course
	if err != nil {
		log.Fatal(err)
		return
	}
}

func ExampleRegisterMigrations() {
	createUserTable := &DummyMigration{
		Ver:       1,
		UpQuery:   "CREATE TABLE IF NOT EXISTS users(id bigint primary key, username varchar);",
		DownQuery: "DROP TABLE IF EXISTS users;",
	}

	key := "myUniqueKey"
	err := inmem.RegisterMigrations(key, createUserTable)
	if err != nil {
		log.Fatal(err)
		return
	}

	m, err := migrate.New("inmem://"+key, "database://foobar")
	if err != nil {
		log.Fatal(err)
		return
	}
	err = m.Up() // run your migrations and handle the errors above of course
	if err != nil {
		log.Fatal(err)
		return
	}
}
