package rqlite

import (
	"fmt"
	"io"
	nurl "net/url"
	"strconv"
	"strings"

	"go.uber.org/atomic"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
	"github.com/rqlite/gorqlite"
)

func init() {
	database.Register("rqlite", &Rqlite{})
}

const (
	// DefaultMigrationsTable defines the default rqlite migrations table
	DefaultMigrationsTable = "schema_migrations"

	// DefaultConnectInsecure defines the default setting for connect insecure
	DefaultConnectInsecure = false
)

// ErrNilConfig is returned if no configuration was passed to WithInstance
var ErrNilConfig = fmt.Errorf("no config")

// ErrBadConfig is returned if configuration was invalid
var ErrBadConfig = fmt.Errorf("bad parameter")

// Config defines the driver configuration
type Config struct {
	// ConnectInsecure sets whether the connection uses TLS. Ineffectual when using WithInstance
	ConnectInsecure bool
	// MigrationsTable configures the migrations table name
	MigrationsTable string
}

type Rqlite struct {
	db       *gorqlite.Connection
	isLocked atomic.Bool

	config *Config
}

// WithInstance creates a rqlite database driver with an existing gorqlite database connection
// and a Config struct
func WithInstance(instance *gorqlite.Connection, config *Config) (database.Driver, error) {
	if config == nil {
		return nil, ErrNilConfig
	}

	// we use the consistency level check as a database ping
	if _, err := instance.ConsistencyLevel(); err != nil {
		return nil, err
	}

	if len(config.MigrationsTable) == 0 {
		config.MigrationsTable = DefaultMigrationsTable
	}

	driver := &Rqlite{
		db:     instance,
		config: config,
	}

	if err := driver.ensureVersionTable(); err != nil {
		return nil, err
	}

	return driver, nil
}

// OpenURL creates a rqlite database driver from a connect URL
func OpenURL(url string) (database.Driver, error) {
	d := &Rqlite{}
	return d.Open(url)
}

func (r *Rqlite) ensureVersionTable() (err error) {
	if err = r.Lock(); err != nil {
		return err
	}

	defer func() {
		if e := r.Unlock(); e != nil {
			if err == nil {
				err = e
			} else {
				err = multierror.Append(err, e)
			}
		}
	}()

	stmts := []string{
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (version uint64, dirty bool)`, r.config.MigrationsTable),
		fmt.Sprintf(`CREATE UNIQUE INDEX IF NOT EXISTS version_unique ON %s (version)`, r.config.MigrationsTable),
	}

	if _, err := r.db.Write(stmts); err != nil {
		return err
	}

	return nil
}

// Open returns a new driver instance configured with parameters
// coming from the URL string. Migrate will call this function
// only once per instance.
func (r *Rqlite) Open(url string) (database.Driver, error) {
	dburl, config, err := parseUrl(url)
	if err != nil {
		return nil, err
	}
	r.config = config

	r.db, err = gorqlite.Open(dburl.String())
	if err != nil {
		return nil, err
	}

	if err := r.ensureVersionTable(); err != nil {
		return nil, err
	}

	return r, nil
}

// Close closes the underlying database instance managed by the driver.
// Migrate will call this function only once per instance.
func (r *Rqlite) Close() error {
	r.db.Close()
	return nil
}

// Lock should acquire a database lock so that only one migration process
// can run at a time. Migrate will call this function before Run is called.
// If the implementation can't provide this functionality, return nil.
// Return database.ErrLocked if database is already locked.
func (r *Rqlite) Lock() error {
	if !r.isLocked.CAS(false, true) {
		return database.ErrLocked
	}
	return nil
}

// Unlock should release the lock. Migrate will call this function after
// all migrations have been run.
func (r *Rqlite) Unlock() error {
	if !r.isLocked.CAS(true, false) {
		return database.ErrNotLocked
	}
	return nil
}

// Run applies a migration to the database. migration is guaranteed to be not nil.
func (r *Rqlite) Run(migration io.Reader) error {
	migr, err := io.ReadAll(migration)
	if err != nil {
		return err
	}

	query := string(migr[:])
	if _, err := r.db.WriteOne(query); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}

	return nil
}

// SetVersion saves version and dirty state.
// Migrate will call this function before and after each call to Run.
// version must be >= -1. -1 means NilVersion.
func (r *Rqlite) SetVersion(version int, dirty bool) error {
	deleteQuery := fmt.Sprintf(`DELETE FROM %s`, r.config.MigrationsTable)
	statements := []gorqlite.ParameterizedStatement{
		{
			Query: deleteQuery,
		},
	}

	// Also re-write the schema version for nil dirty versions to prevent
	// empty schema version for failed down migration on the first migration
	// See: https://github.com/golang-migrate/migrate/issues/330
	insertQuery := fmt.Sprintf(`INSERT INTO %s (version, dirty) VALUES (?, ?)`, r.config.MigrationsTable)
	if version >= 0 || (version == database.NilVersion && dirty) {
		statements = append(statements, gorqlite.ParameterizedStatement{
			Query: insertQuery,
			Arguments: []interface{}{
				version,
				dirty,
			},
		})
	}

	wr, err := r.db.WriteParameterized(statements)
	if err != nil {
		for i, res := range wr {
			if res.Err != nil {
				return &database.Error{OrigErr: err, Query: []byte(statements[i].Query)}
			}
		}

		// if somehow we're still here, return the original error with combined queries
		return &database.Error{OrigErr: err, Query: []byte(deleteQuery + "\n" + insertQuery)}
	}

	return nil
}

// Version returns the currently active version and if the database is dirty.
// When no migration has been applied, it must return version -1.
// Dirty means, a previous migration failed and user interaction is required.
func (r *Rqlite) Version() (version int, dirty bool, err error) {
	query := "SELECT version, dirty FROM " + r.config.MigrationsTable + " LIMIT 1"

	qr, err := r.db.QueryOne(query)
	if err != nil {
		return database.NilVersion, false, nil
	}

	if !qr.Next() {
		return database.NilVersion, false, nil
	}

	if err := qr.Scan(&version, &dirty); err != nil {
		return database.NilVersion, false, &database.Error{OrigErr: err, Query: []byte(query)}
	}

	return version, dirty, nil
}

// Drop deletes everything in the database.
// Note that this is a breaking action, a new call to Open() is necessary to
// ensure subsequent calls work as expected.
func (r *Rqlite) Drop() error {
	query := `SELECT name FROM sqlite_master WHERE type = 'table'`

	tables, err := r.db.QueryOne(query)
	if err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}

	statements := make([]string, 0)
	for tables.Next() {
		var tableName string
		if err := tables.Scan(&tableName); err != nil {
			return err
		}

		if len(tableName) > 0 {
			statement := fmt.Sprintf(`DROP TABLE %s`, tableName)
			statements = append(statements, statement)
		}
	}

	// return if nothing to do
	if len(statements) <= 0 {
		return nil
	}

	wr, err := r.db.Write(statements)
	if err != nil {
		for i, res := range wr {
			if res.Err != nil {
				return &database.Error{OrigErr: err, Query: []byte(statements[i])}
			}
		}

		// if somehow we're still here, return the original error with combined queries
		return &database.Error{OrigErr: err, Query: []byte(strings.Join(statements, "\n"))}
	}

	return nil
}

func parseUrl(url string) (*nurl.URL, *Config, error) {
	parsedUrl, err := nurl.Parse(url)
	if err != nil {
		return nil, nil, err
	}

	config, err := parseConfigFromQuery(parsedUrl.Query())
	if err != nil {
		return nil, nil, err
	}

	if parsedUrl.Scheme != "rqlite" {
		return nil, nil, errors.Wrap(ErrBadConfig, "bad scheme")
	}

	// adapt from rqlite to http/https schemes
	if config.ConnectInsecure {
		parsedUrl.Scheme = "http"
	} else {
		parsedUrl.Scheme = "https"
	}

	filteredUrl := migrate.FilterCustomQuery(parsedUrl)

	return filteredUrl, config, nil
}

func parseConfigFromQuery(queryVals nurl.Values) (*Config, error) {
	c := Config{
		ConnectInsecure: DefaultConnectInsecure,
		MigrationsTable: DefaultMigrationsTable,
	}

	migrationsTable := queryVals.Get("x-migrations-table")
	if migrationsTable != "" {
		if strings.HasPrefix(migrationsTable, "sqlite_") {
			return nil, errors.Wrap(ErrBadConfig, "invalid value for x-migrations-table")
		}
		c.MigrationsTable = migrationsTable
	}

	connectInsecureStr := queryVals.Get("x-connect-insecure")
	if connectInsecureStr != "" {
		connectInsecure, err := strconv.ParseBool(connectInsecureStr)
		if err != nil {
			return nil, errors.Wrap(ErrBadConfig, "invalid value for x-connect-insecure")
		}
		c.ConnectInsecure = connectInsecure
	}

	return &c, nil
}
