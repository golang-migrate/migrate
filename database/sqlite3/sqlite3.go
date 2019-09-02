package sqlite3

import (
	"database/sql"
	"fmt"
	"io"
	"io/ioutil"
	nurl "net/url"
	"strings"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
	"github.com/hashicorp/go-multierror"
	_ "github.com/mattn/go-sqlite3"
)

func init() {
	database.Register("sqlite3", &Sqlite{})
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
}

type Sqlite struct {
	db       *sql.DB
	isLocked bool

	config *Config
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

	mx := &Sqlite{
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
func (m *Sqlite) ensureVersionTable() (err error) {
	if err = m.Lock(); err != nil {
		return err
	}

	defer func() {
		if e := m.Unlock(); e != nil {
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

	if _, err := m.db.Exec(query); err != nil {
		return err
	}
	return nil
}

func (m *Sqlite) Open(url string) (database.Driver, error) {
	purl, err := nurl.Parse(url)
	if err != nil {
		return nil, err
	}
	dbfile := strings.Replace(migrate.FilterCustomQuery(purl).String(), "sqlite3://", "", 1)
	db, err := sql.Open("sqlite3", dbfile)
	if err != nil {
		return nil, err
	}

	migrationsTable := purl.Query().Get("x-migrations-table")
	if len(migrationsTable) == 0 {
		migrationsTable = DefaultMigrationsTable
	}
	mx, err := WithInstance(db, &Config{
		DatabaseName:    purl.Path,
		MigrationsTable: migrationsTable,
	})
	if err != nil {
		return nil, err
	}
	return mx, nil
}

func (m *Sqlite) Close() error {
	return m.db.Close()
}

func (m *Sqlite) Drop() (err error) {
	query := `SELECT name FROM sqlite_master WHERE type = 'table';`
	tables, err := m.db.Query(query)
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
	if len(tableNames) > 0 {
		for _, t := range tableNames {
			query := "DROP TABLE " + t
			err = m.executeQuery(query)
			if err != nil {
				return &database.Error{OrigErr: err, Query: []byte(query)}
			}
		}
		query := "VACUUM"
		_, err = m.db.Query(query)
		if err != nil {
			return &database.Error{OrigErr: err, Query: []byte(query)}
		}
	}

	return nil
}

func (m *Sqlite) Lock() error {
	if m.isLocked {
		return database.ErrLocked
	}
	m.isLocked = true
	return nil
}

func (m *Sqlite) Unlock() error {
	if !m.isLocked {
		return nil
	}
	m.isLocked = false
	return nil
}

func (m *Sqlite) Run(migration io.Reader) error {
	migr, err := ioutil.ReadAll(migration)
	if err != nil {
		return err
	}
	query := string(migr[:])

	return m.executeQuery(query)
}

func (m *Sqlite) executeQuery(query string) error {
	tx, err := m.db.Begin()
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

func (m *Sqlite) SetVersion(version int, dirty bool) error {
	tx, err := m.db.Begin()
	if err != nil {
		return &database.Error{OrigErr: err, Err: "transaction start failed"}
	}

	query := "DELETE FROM " + m.config.MigrationsTable
	if _, err := tx.Exec(query); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}

	if version >= 0 {
		query := fmt.Sprintf(`INSERT INTO %s (version, dirty) VALUES (%d, '%t')`, m.config.MigrationsTable, version, dirty)
		if _, err := tx.Exec(query); err != nil {
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

func (m *Sqlite) Version() (version int, dirty bool, err error) {
	query := "SELECT version, dirty FROM " + m.config.MigrationsTable + " LIMIT 1"
	err = m.db.QueryRow(query).Scan(&version, &dirty)
	if err != nil {
		return database.NilVersion, false, nil
	}
	return version, dirty, nil
}
