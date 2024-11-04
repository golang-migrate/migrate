package ydb

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"sync/atomic"

	"github.com/hashicorp/go-multierror"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"

	"github.com/golang-migrate/migrate/v4"
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
	if err := db.ensureVersionTable(); err != nil {
		return nil, err
	}
	return db, nil
}

func (db *YDB) Open(dsn string) (database.Driver, error) {
	purl, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}

	pquery, err := url.ParseQuery(purl.RawQuery)
	if err != nil {
		return nil, err
	}

	purl.Scheme = "grpc"
	if pquery.Has("x-use-grpcs") {
		purl.Scheme = "grpcs"
	}

	purl = migrate.FilterCustomQuery(purl)
	fmt.Println(purl, pquery)

	driver, err := ydb.Open(context.TODO(), purl.String(), ydb.With())
	if err != nil {
		return nil, err
	}

	px, err := WithInstance(driver, &Config{
		MigrationsTable: pquery.Get("x-migrations-table"),
	})
	if err != nil {
		return nil, err
	}
	return px, nil
}

func (db *YDB) Close() error {
	return db.driver.Close(context.TODO())
}

func (db *YDB) Run(migration io.Reader) error {
	rawMigrations, err := io.ReadAll(migration)
	if err != nil {
		return err
	}

	res, err := db.driver.Scripting().Execute(context.TODO(), string(rawMigrations), nil)
	if err != nil {
		return err
	}
	return res.Close()
}

func (db *YDB) SetVersion(version int, dirty bool) error {
	deleteVersionQuery := fmt.Sprintf(`
		DELETE FROM %s 
	`, db.config.MigrationsTable)

	insertVersionQuery := fmt.Sprintf(`
		INSERT INTO %s (version, dirty, created) VALUES (%d, %t, CurrentUtcTimestamp())
	`, db.config.MigrationsTable, version, dirty)

	ctx := context.TODO()
	err := db.driver.Query().DoTx(ctx, func(ctx context.Context, tx query.TxActor) error {
		if err := tx.Exec(ctx, deleteVersionQuery); err != nil {
			return err
		}
		// Also re-write the schema version for nil dirty versions to prevent
		// empty schema version for failed down migration on the first migration
		// See: https://github.com/golang-migrate/migrate/issues/330
		if version >= 0 || (version == database.NilVersion && dirty) {
			if err := tx.Exec(ctx, insertVersionQuery); err != nil {
				return err
			}
		}
		return nil
	}, query.WithTxSettings(query.TxSettings(query.WithSerializableReadWrite())))
	return err
}

func (db *YDB) Version() (version int, dirty bool, err error) {
	getVersionQuery := fmt.Sprintf(`
		SELECT version, dirty FROM %s ORDER BY version DESC LIMIT 1
	`, db.config.MigrationsTable)

	rs, err := db.driver.Query().QueryResultSet(context.TODO(), getVersionQuery)
	if err != nil {
		return 0, false, &database.Error{OrigErr: err, Query: []byte(getVersionQuery)}
	}

	row, err := rs.NextRow(context.TODO())
	if err != nil {
		if errors.Is(err, io.EOF) {
			return database.NilVersion, false, nil
		}
		return 0, false, err
	}

	var v uint64
	if err = row.Scan(&v, &dirty); err != nil {
		return 0, false, &database.Error{OrigErr: err, Query: []byte(getVersionQuery)}
	}
	return int(v), dirty, err
}

func (db *YDB) Drop() (err error) {
	ctx := context.TODO()

	listQuery := "SELECT DISTINCT Path FROM `.sys/partition_stats` WHERE Path NOT LIKE '%/.sys%'"
	rs, err := db.driver.Query().QueryResultSet(context.TODO(), listQuery)
	if err != nil {
		return &database.Error{OrigErr: err, Query: []byte(listQuery)}
	}

	for {
		var row query.Row
		if row, err = rs.NextRow(ctx); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}

		var table string
		if err = row.Scan(&table); err != nil {
			return err
		}

		dropQuery := fmt.Sprintf("DROP TABLE %s", table)
		if err = db.driver.Query().Exec(ctx, dropQuery); err != nil {
			return &database.Error{OrigErr: err, Query: []byte(dropQuery)}
		}
	}
	return err
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
	if err = db.Lock(); err != nil {
		return err
	}

	defer func() {
		if unlockErr := db.Unlock(); unlockErr != nil {
			if err == nil {
				err = unlockErr
			} else {
				err = multierror.Append(err, unlockErr)
			}
		}
	}()

	createVersionTableQuery := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			version Uint64,
			dirty Bool,
			created Timestamp,
			PRIMARY KEY(version)
		)
	`, db.config.MigrationsTable)
	err = db.driver.Query().Exec(context.TODO(), createVersionTableQuery)
	if err != nil {
		return err
	}
	return err
}
