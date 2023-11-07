package libsql

import (
	"database/sql"
	"fmt"
	"io"

	nurl "net/url"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
	"github.com/hashicorp/go-multierror"
	"go.uber.org/atomic"

	_ "github.com/libsql/libsql-client-go/libsql"
	_ "modernc.org/sqlite"
)

func init() {
	database.Register("libsql", &Libsql{})
}

var DefaultMigrationsTable = "schema_migrations"
var (
	ErrDatabaseDirty      = fmt.Errorf("database is dirty")
	ErrNilConfig          = fmt.Errorf("no config")
	ErrNoDatabaseName     = fmt.Errorf("no database name")
	ErrDatabaseURLInvalid = fmt.Errorf("invalid database url")
)

type Config struct {
	MigrationsTable string

	DatabaseURL string
}

type Libsql struct {
	db       *sql.DB
	isLocked atomic.Bool

	config *Config
}

var _ database.Driver = (*Libsql)(nil)

func WithInstance(instance *sql.DB, config *Config) (database.Driver, error) {
	if config == nil {
		return nil, ErrNilConfig
	}

	if err := instance.Ping(); err != nil {
		return nil, err
	}

	if len(config.MigrationsTable) == 0 {
		config.MigrationsTable = DefaultMigrationsTable
	}

	lx := &Libsql{
		db:     instance,
		config: config,
	}
	if err := lx.ensureVersionTable(); err != nil {
		return nil, err
	}
	return lx, nil
}

func (l *Libsql) ensureVersionTable() (err error) {
	if err = l.Lock(); err != nil {
		return err
	}

	defer func() {
		if e := l.Unlock(); e != nil {
			if err == nil {
				err = e
			} else {
				err = multierror.Append(err, e)
			}
		}
	}()

	query := fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %s (version uint64,dirty bool);
  CREATE UNIQUE INDEX IF NOT EXISTS version_unique ON %s (version);
  `, l.config.MigrationsTable, l.config.MigrationsTable)

	if _, err := l.db.Exec(query); err != nil {
		return err
	}
	return nil
}

// Close implements database.Driver.
func (l *Libsql) Close() error {
	return l.db.Close()
}

func (l *Libsql) Drop() (err error) {
	query := `SELECT name FROM sqlite_master WHERE type = 'table';`
	tables, err := l.db.Query(query)
	if err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}
	defer func() {
		if errClose := tables.Close(); errClose != nil {
			err = multierror.Append(err, errClose)
		}
	}()

	tableNames := make([]string, 0)
	for tables.Next() {
		var tableName string
		if err := tables.Scan(&tableName); err != nil {
			return err
		}
		if len(tableName) > 0 {
			tableNames = append(tableNames, tableName)
		}
	}
	if err := tables.Err(); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}

	if len(tableNames) > 0 {
		for _, t := range tableNames {
			query := "DROP TABLE " + t
			err = l.executeQuery(query)
			if err != nil {
				return &database.Error{OrigErr: err, Query: []byte(query)}
			}
		}
	}

	return nil
}

func (l *Libsql) Lock() error {
	if !l.isLocked.CAS(false, true) {
		return database.ErrLocked
	}
	return nil
}

func (l *Libsql) Unlock() error {
	if !l.isLocked.CAS(true, false) {
		return database.ErrNotLocked
	}
	return nil
}

func (l *Libsql) Open(url string) (database.Driver, error) {
	purl, err := nurl.Parse(url)
	if err != nil {
		return nil, err
	}

	dbUrl := migrate.FilterCustomQuery(purl).String()
	db, err := sql.Open("libsql", dbUrl)
	if err != nil {
		return nil, err
	}

	qv := purl.Query()
	migrationsTable := qv.Get("x-migrations-table")
	if len(migrationsTable) == 0 {
		migrationsTable = DefaultMigrationsTable
	}

	lx, err := WithInstance(db, &Config{
		DatabaseURL:     dbUrl,
		MigrationsTable: migrationsTable,
	})
	if err != nil {
		return nil, err
	}

	return lx, nil
}

func (l *Libsql) Run(migration io.Reader) error {
	migr, err := io.ReadAll(migration)
	if err != nil {
		return err
	}
	query := string(migr[:])

	return l.executeQuery(query)
}

func (l *Libsql) SetVersion(version int, dirty bool) error {
	tx, err := l.db.Begin()
	if err != nil {
		return &database.Error{OrigErr: err, Err: "transaction start failed"}
	}

	query := "DELETE FROM " + l.config.MigrationsTable
	if _, err := tx.Exec(query); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}

	// Also re-write the schema version for nil dirty versions to prevent
	// empty schema version for failed down migration on the first migration
	// See: https://github.com/golang-migrate/migrate/issues/330
	if version >= 0 || (version == database.NilVersion && dirty) {
		query := fmt.Sprintf(`INSERT INTO %s (version, dirty) VALUES (?, ?)`, l.config.MigrationsTable)
		if _, err := tx.Exec(query, version, dirty); err != nil {
			if errRollback := tx.Rollback(); errRollback != nil {
				err = multierror.Append(err, errRollback)
			}
			return &database.Error{OrigErr: err, Query: []byte(query)}
		}
	}

	if err := tx.Commit(); err != nil {
		return &database.Error{OrigErr: err, Err: "transaction commit failed"}
	}

	return nil
}

func (l *Libsql) executeQuery(query string) error {
	tx, err := l.db.Begin()
	if err != nil {
		return &database.Error{OrigErr: err, Err: "transaction start failed"}
	}
	if _, err := tx.Exec(query); err != nil {
		if errRollback := tx.Rollback(); errRollback != nil {
			err = multierror.Append(err, errRollback)
		}
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}
	if err := tx.Commit(); err != nil {
		return &database.Error{OrigErr: err, Err: "transaction commit failed"}
	}
	return nil
}

func (l *Libsql) Version() (version int, dirty bool, err error) {
	query := "SELECT version, dirty FROM " + l.config.MigrationsTable + " LIMIT 1"
	err = l.db.QueryRow(query).Scan(&version, &dirty)
	if err != nil {
		return database.NilVersion, false, nil
	}
	return version, dirty, nil
}
