package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	nurl "net/url"
	"strconv"
	"strings"

	"go.uber.org/atomic"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
	"github.com/hashicorp/go-multierror"
	_ "modernc.org/sqlite"
)

func init() {
	database.Register("sqlite", &Sqlite{})
}

var DefaultMigrationsTable = "schema_migrations"
var (
	ErrDatabaseDirty  = fmt.Errorf("database is dirty")
	ErrNilConfig      = fmt.Errorf("no config")
	ErrNoDatabaseName = fmt.Errorf("no database name")
)

type Config struct {
	MigrationsTable string
	DatabaseName    string
	NoTxWrap        bool
}

type Sqlite struct {
	db       *sql.DB
	isLocked atomic.Bool

	config *Config
}

func WithInstance(ctx context.Context, instance *sql.DB, config *Config) (database.Driver, error) {
	if config == nil {
		return nil, ErrNilConfig
	}

	if err := instance.Ping(); err != nil {
		return nil, err
	}

	if len(config.MigrationsTable) == 0 {
		config.MigrationsTable = DefaultMigrationsTable
	}

	mx := &Sqlite{
		db:     instance,
		config: config,
	}
	if err := mx.ensureVersionTable(ctx); err != nil {
		return nil, err
	}
	return mx, nil
}

// ensureVersionTable checks if versions table exists and, if not, creates it.
// Note that this function locks the database, which deviates from the usual
// convention of "caller locks" in the Sqlite type.
func (m *Sqlite) ensureVersionTable(ctx context.Context) (err error) {
	if err = m.Lock(ctx); err != nil {
		return err
	}

	defer func() {
		if e := m.Unlock(ctx); e != nil {
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
  `, m.config.MigrationsTable, m.config.MigrationsTable)

	if _, err := m.db.ExecContext(ctx, query); err != nil {
		return err
	}
	return nil
}

func (m *Sqlite) Open(ctx context.Context, url string) (database.Driver, error) {
	purl, err := nurl.Parse(url)
	if err != nil {
		return nil, err
	}
	dbfile := strings.Replace(migrate.FilterCustomQuery(purl).String(), "sqlite://", "", 1)
	db, err := sql.Open("sqlite", dbfile)
	if err != nil {
		return nil, err
	}

	qv := purl.Query()

	migrationsTable := qv.Get("x-migrations-table")
	if len(migrationsTable) == 0 {
		migrationsTable = DefaultMigrationsTable
	}

	noTxWrap := false
	if v := qv.Get("x-no-tx-wrap"); v != "" {
		noTxWrap, err = strconv.ParseBool(v)
		if err != nil {
			return nil, fmt.Errorf("x-no-tx-wrap: %s", err)
		}
	}

	mx, err := WithInstance(ctx, db, &Config{
		DatabaseName:    purl.Path,
		MigrationsTable: migrationsTable,
		NoTxWrap:        noTxWrap,
	})
	if err != nil {
		return nil, err
	}
	return mx, nil
}

func (m *Sqlite) Close(ctx context.Context) error {
	return m.db.Close()
}

func (m *Sqlite) Drop(ctx context.Context) (err error) {
	query := `SELECT name FROM sqlite_master WHERE type = 'table';`
	tables, err := m.db.QueryContext(ctx, query)
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
			err = m.executeQuery(ctx, query)
			if err != nil {
				return &database.Error{OrigErr: err, Query: []byte(query)}
			}
		}
		query := "VACUUM"
		_, err = m.db.QueryContext(ctx, query)
		if err != nil {
			return &database.Error{OrigErr: err, Query: []byte(query)}
		}
	}

	return nil
}

func (m *Sqlite) Lock(ctx context.Context) error {
	if !m.isLocked.CAS(false, true) {
		return database.ErrLocked
	}
	return nil
}

func (m *Sqlite) Unlock(ctx context.Context) error {
	if !m.isLocked.CAS(true, false) {
		return database.ErrNotLocked
	}
	return nil
}

func (m *Sqlite) Run(ctx context.Context, migration io.Reader) error {
	migr, err := io.ReadAll(migration)
	if err != nil {
		return err
	}
	query := string(migr[:])

	if m.config.NoTxWrap {
		return m.executeQueryNoTx(ctx, query)
	}
	return m.executeQuery(ctx, query)
}

func (m *Sqlite) executeQuery(ctx context.Context, query string) error {
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return &database.Error{OrigErr: err, Err: "transaction start failed"}
	}
	if _, err := tx.ExecContext(ctx, query); err != nil {
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

func (m *Sqlite) executeQueryNoTx(ctx context.Context, query string) error {
	if _, err := m.db.ExecContext(ctx, query); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}
	return nil
}

func (m *Sqlite) SetVersion(ctx context.Context, version int, dirty bool) error {
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return &database.Error{OrigErr: err, Err: "transaction start failed"}
	}

	query := "DELETE FROM " + m.config.MigrationsTable
	if _, err := tx.ExecContext(ctx, query); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}

	// Also re-write the schema version for nil dirty versions to prevent
	// empty schema version for failed down migration on the first migration
	// See: https://github.com/golang-migrate/migrate/issues/330
	if version >= 0 || (version == database.NilVersion && dirty) {
		query := fmt.Sprintf(`INSERT INTO %s (version, dirty) VALUES (?, ?)`, m.config.MigrationsTable)
		if _, err := tx.ExecContext(ctx, query, version, dirty); err != nil {
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

func (m *Sqlite) Version(ctx context.Context) (version int, dirty bool, err error) {
	query := "SELECT version, dirty FROM " + m.config.MigrationsTable + " LIMIT 1"
	err = m.db.QueryRow(query).Scan(&version, &dirty)
	if err != nil {
		return database.NilVersion, false, nil
	}
	return version, dirty, nil
}
