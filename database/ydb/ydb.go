package ydb

import (
	"context"
	"fmt"
	"io"
	"sync/atomic"

	"github.com/ydb-platform/ydb-go-sdk/v3"

	"github.com/golang-migrate/migrate/v4/database"
)

func init() {
	db := YDB{}
	database.Register("ydb", &db)
}

const DefaultMigrationsTable = "schema_migrations"

var (
	ErrDatabaseDirty  = fmt.Errorf("database is dirty")
	ErrNilConfig      = fmt.Errorf("no config")
	ErrNoDatabaseName = fmt.Errorf("no database name")
)

type Config struct {
	MigrationsTable string
}

type YDB struct {
	driver *ydb.Driver
	config *Config

	isLocked atomic.Bool
}

func WithInstance(driver *ydb.Driver, config *Config) (database.Driver, error) {
	if config == nil {
		return nil, ErrNilConfig
	}

	if len(config.MigrationsTable) == 0 {
		config.MigrationsTable = DefaultMigrationsTable
	}

	db := &YDB{
		driver: driver,
		config: config,
	}
	err := db.ensureVersionTable()
	if err != nil {
		return nil, err
	}

	return db, nil
}

func (db *YDB) Open(url string) (database.Driver, error) {
	return nil, nil
}

func (db *YDB) Close() error {
	return db.driver.Close(context.Background())
}

func (db *YDB) Run(migration io.Reader) error {
	return nil
}

func (db *YDB) SetVersion(version int, dirty bool) error {
	return nil
}

func (db *YDB) Version() (version int, dirty bool, err error) {
	return
}

func (db *YDB) Drop() error {
	return nil
}

func (db *YDB) Lock() error {
	if !db.isLocked.CompareAndSwap(false, true) {
		return database.ErrLocked
	}
	return nil
}

func (db *YDB) Unlock() error {
	if !db.isLocked.CompareAndSwap(true, false) {
		return database.ErrNotLocked
	}
	return nil
}

// ensureVersionTable checks if versions table exists and, if not, creates it.
func (db *YDB) ensureVersionTable() (err error) {
	return nil
}
