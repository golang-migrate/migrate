package libsql

import (
	"database/sql"
	"fmt"
	"io"
	nurl "net/url"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
	"github.com/hashicorp/go-multierror"

	_ "github.com/tursodatabase/libsql-client-go/libsql"
)

func init() {
	database.Register("libsql", &LibSQL{})
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

type LibSQL struct {
	db       *sql.DB
	isLocked atomic.Bool

	config *Config
}

func (d *LibSQL) Open(url string) (database.Driver, error) {
	purl, err := nurl.Parse(url)
	if err != nil {
		return nil, err
	}

	dbfile := strings.Replace(migrate.FilterCustomQuery(purl).String(), "libsql://", "", 1)

	if strings.HasPrefix(dbfile, "file://") {
		return nil, fmt.Errorf("invalid URL: %s file:// is not supported", dbfile)
	}

	db, err := sql.Open("libsql", dbfile)
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

	mx, err := WithInstance(db, &Config{
		DatabaseName:    purl.Path,
		MigrationsTable: migrationsTable,
		NoTxWrap:        noTxWrap,
	})
	if err != nil {
		return nil, err
	}
	return mx, nil
}

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

	mx := &LibSQL{
		db:     instance,
		config: config,
	}
	if err := mx.ensureVersionTable(); err != nil {
		return nil, err
	}
	return mx, nil
}

// ensureVersionTable checks if versions table exists and, if not, creates it.
// Note that this function locks the database, which deviates from the usual
// convention of "caller locks" in the Sqlite type.
func (d *LibSQL) ensureVersionTable() (err error) {
	if err = d.Lock(); err != nil {
		return err
	}

	defer func() {
		if e := d.Unlock(); e != nil {
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
  `, d.config.MigrationsTable, d.config.MigrationsTable)

	if _, err := d.db.Exec(query); err != nil {
		return err
	}
	return nil
}

func (d *LibSQL) Close() error {
	if d.db != nil {
		return d.db.Close()
	}
	return nil
}

func (d *LibSQL) Lock() error {
	if !d.isLocked.CompareAndSwap(false, true) {
		return database.ErrLocked
	}
	return nil
}

func (d *LibSQL) Unlock() error {
	if !d.isLocked.CompareAndSwap(true, false) {
		return database.ErrNotLocked
	}
	return nil
}

func (d *LibSQL) Run(migration io.Reader) error {
	migr, err := io.ReadAll(migration)
	if err != nil {
		return err
	}
	query := string(migr[:])

	if d.config.NoTxWrap {
		return d.executeQueryNoTx(query)
	}
	return d.executeQuery(query)
}

func (d *LibSQL) executeQuery(query string) error {
	tx, err := d.db.Begin()
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

func (d *LibSQL) executeQueryNoTx(query string) error {
	if _, err := d.db.Exec(query); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}
	return nil
}

func (d *LibSQL) SetVersion(version int, dirty bool) error {
	tx, err := d.db.Begin()
	if err != nil {
		return &database.Error{OrigErr: err, Err: "transaction start failed"}
	}

	query := "DELETE FROM " + d.config.MigrationsTable
	if _, err := tx.Exec(query); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}

	// Also re-write the schema version for nil dirty versions to prevent
	// empty schema version for failed down migration on the first migration
	// See: https://github.com/golang-migrate/migrate/issues/330
	if version >= 0 || (version == database.NilVersion && dirty) {
		query := fmt.Sprintf(`INSERT INTO %s (version, dirty) VALUES (?, ?)`, d.config.MigrationsTable)
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

func (d *LibSQL) Version() (version int, dirty bool, err error) {
	query := "SELECT version, dirty FROM " + d.config.MigrationsTable + " LIMIT 1"
	q := d.db.QueryRow(query)
	err = q.Scan(&version, &dirty)

	if err != nil {
		return database.NilVersion, false, nil
	}
	return version, dirty, nil
}

func (d *LibSQL) Drop() (err error) {
	if err := d.dropViews(); err != nil {
		return err
	}

	if err := d.dropTables(); err != nil {
		return err
	}

	return nil
}

func (d *LibSQL) dropViews() (err error) {
	query := `SELECT name FROM sqlite_master WHERE type = 'view';`
	views, err := d.db.Query(query)
	if err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}
	defer func() {
		if errClose := views.Close(); errClose != nil {
			err = multierror.Append(err, errClose)
		}
	}()

	viewNames := make([]string, 0)
	for views.Next() {
		var viewName string
		if err := views.Scan(&viewName); err != nil {
			return err
		}
		if err := views.Err(); err != nil {
			return &database.Error{OrigErr: err, Query: []byte(query)}
		}

		viewNames = append(viewNames, viewName)
	}

	for _, v := range viewNames {
		query := "DROP VIEW " + v
		err = d.executeQuery(query)
		if err != nil {
			return &database.Error{OrigErr: err, Query: []byte(query)}
		}
	}

	return nil
}

func (d *LibSQL) dropTables() (err error) {
	query := `SELECT name FROM sqlite_master WHERE type = 'table';`
	tables, err := d.db.Query(query)
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

		if err := tables.Err(); err != nil {
			return &database.Error{OrigErr: err, Query: []byte(query)}
		}

		tableNames = append(tableNames, tableName)
	}

	// We range over the tables in reverse order to avoid hitting foreign key constraints
	for i := len(tableNames) - 1; i >= 0; i-- {
		t := tableNames[i]

		// table sqlite_sequence may not be dropped
		if t == "sqlite_sequence" {
			continue
		}

		query := "DROP TABLE " + t
		err = d.executeQuery(query)
		if err != nil {
			return &database.Error{OrigErr: err, Query: []byte(query)}
		}
	}

	return nil
}
