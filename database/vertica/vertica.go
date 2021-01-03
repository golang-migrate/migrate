package vertica

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"io/ioutil"
	nurl "net/url"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
	multierror "github.com/hashicorp/go-multierror"
	_ "github.com/vertica/vertica-sql-go"
)

func init() {
	database.Register("vertica", &Vertica{})
}

var DefaultMigrationsTable = "schema_migrations"

var (
	ErrDatabaseDirty    = fmt.Errorf("database is dirty")
	ErrNilConfig        = fmt.Errorf("no config")
	ErrAppendPEM        = fmt.Errorf("failed to append PEM")
	ErrTLSCertKeyConfig = fmt.Errorf("To use TLS client authentication, both x-tls-cert and x-tls-key must not be empty")
)

type Config struct {
	MigrationsTable string
	Schema          string
}

type Vertica struct {
	conn   *sql.Conn
	db     *sql.DB
	config *Config
}

func (v *Vertica) Open(url string) (database.Driver, error) {
	purl, err := nurl.Parse(url)

	if err != nil {
		return nil, err
	}

	db, err := sql.Open("vertica", migrate.FilterCustomQuery(purl).String())
	if err != nil {
		return nil, err
	}

	migrationsTable := purl.Query().Get("x-migrations-table")
	if len(migrationsTable) == 0 {
		migrationsTable = DefaultMigrationsTable
	}
	schema := purl.Query().Get("x-schema")

	if schema == "" {
		schema = "public"
	}

	vx, err := WithInstance(db, &Config{
		Schema:          schema,
		MigrationsTable: migrationsTable,
	})

	if err != nil {
		return nil, err
	}

	return vx, nil
}

func (v *Vertica) Close() error {
	if e := v.conn.Close(); e != nil {
		return fmt.Errorf("error closing vertica connection: %w", e)
	}

	if e := v.db.Close(); e != nil {
		return fmt.Errorf("error closing vertica db: %w", e)
	}
	return nil
}

func (v *Vertica) dropSchema() error {
	query := fmt.Sprintf("DROP SCHEMA %s CASCADE", v.config.Schema)
	_, e := v.conn.QueryContext(context.Background(), query)
	return e
}

func (v *Vertica) dropTables() error {

	schema, err := v.configuredSchema()
	if err != nil {
		return err
	}
	query := fmt.Sprintf(`SELECT TABLE_NAME FROM v_catalog.tables WHERE TABLE_SCHEMA = '%s'`, schema)

	tables, err := v.conn.QueryContext(context.Background(), query)
	if err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}
	defer func() {
		if errClose := tables.Close(); errClose != nil {
			err = multierror.Append(err, errClose)
		}
	}()

	// delete one table after another
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
		// delete one by one ...
		for _, t := range tableNames {
			query = "DROP TABLE " + t + " CASCADE"
			if _, err := v.conn.ExecContext(context.Background(), query); err != nil {
				return &database.Error{OrigErr: err, Query: []byte(query)}
			}
		}
	}

	return nil
}

func (v *Vertica) dropViews() error {
	query := fmt.Sprintf(`SELECT TABLE_NAME FROM v_catalog.views WHERE TABLE_SCHEMA = '%s' `, v.config.Schema)
	tables, err := v.conn.QueryContext(context.Background(), query)
	if err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}
	if err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}
	defer func() {
		if errClose := tables.Close(); errClose != nil {
			err = multierror.Append(err, errClose)
		}
	}()

	// delete one table after another
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

		// delete one by one ...
		for _, t := range tableNames {
			query = "DROP VIEW " + t
			if _, err := v.conn.ExecContext(context.Background(), query); err != nil {
				return &database.Error{OrigErr: err, Query: []byte(query)}
			}
		}
	}

	return nil
}

func (v *Vertica) Drop() error {

	err := v.dropTables()
	if err != nil {
		return err
	}
	return v.dropViews()
}

func (v *Vertica) Lock() error {
	return nil
}

func (v *Vertica) Unlock() error {
	return nil
}

func (v *Vertica) Run(migration io.Reader) error {
	migr, err := ioutil.ReadAll(migration)
	if err != nil {
		return err
	}

	query := string(migr[:])
	if _, err := v.conn.ExecContext(context.Background(), query); err != nil {
		return database.Error{OrigErr: err, Err: "migration failed", Query: migr}
	}

	return nil
}

func (v *Vertica) SetVersion(version int, dirty bool) error {
	tx, err := v.conn.BeginTx(context.Background(), &sql.TxOptions{})
	if err != nil {
		return &database.Error{OrigErr: err, Err: "transaction start failed"}
	}

	query := `TRUNCATE TABLE ` + v.config.MigrationsTable
	if _, err := tx.Exec(query); err != nil {
		if errRollback := tx.Rollback(); errRollback != nil {
			err = multierror.Append(err, errRollback)
		}
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}

	// Also re-write the schema version for nil dirty versions to prevent
	// empty schema version for failed down migration on the first migration
	// See: https://github.com/golang-migrate/migrate/issues/330
	if version >= 0 || (version == database.NilVersion && dirty) {
		var dirtyBit int
		if dirty {
			dirtyBit = 1
		}
		query = `INSERT INTO ` + v.config.MigrationsTable + ` (version, dirty) VALUES (?, ?)`
		if _, err := tx.Exec(query, version, dirtyBit); err != nil {
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

func (v *Vertica) Version() (version int, dirty bool, err error) {
	query := `SELECT version, dirty FROM ` + v.config.MigrationsTable + ` LIMIT 1`
	err = v.conn.QueryRowContext(context.Background(), query).Scan(&version, &dirty)
	switch {
	case err == sql.ErrNoRows:
		return database.NilVersion, false, nil

	case err != nil:
		return 0, false, &database.Error{OrigErr: err, Query: []byte(query)}

	default:
		return version, dirty, nil
	}
}

func WithInstance(instance *sql.DB, config *Config) (database.Driver, error) {
	if config == nil {
		return nil, ErrNilConfig
	}
	conn, err := instance.Conn(context.Background())
	if err != nil {
		return nil, err
	}

	vx := &Vertica{
		conn:   conn,
		db:     instance,
		config: config,
	}

	if err := vx.Ping(); err != nil {
		return nil, err
	}
	if err := vx.setSchema(); err != nil {
		return nil, err
	}

	if err := vx.ensureVersionTable(); err != nil {
		return nil, err
	}

	return vx, nil
}

func (v *Vertica) Ping() error {
	ctx := context.Background()
	return v.conn.PingContext(ctx)
}
func (v *Vertica) setSchema() (err error) {
	if len(v.config.Schema) == 0 {
		return nil
	}

	if err = v.Lock(); err != nil {
		return err
	}

	defer func() {
		if e := v.Unlock(); e != nil {
			if err == nil {
				err = e
			} else {
				err = multierror.Append(err, e)
			}
		}
	}()

	query := "SET SEARCH_PATH = '" + v.config.Schema + "'"
	if _, err := v.conn.ExecContext(context.Background(), query); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}
	return nil
}

// ensureVersionTable checks if versions table exists and, if not, creates it.
func (v *Vertica) ensureVersionTable() (err error) {
	if err = v.Lock(); err != nil {
		return err
	}

	defer func() {
		if e := v.Unlock(); e != nil {
			if err == nil {
				err = e
			} else {
				err = multierror.Append(err, e)
			}
		}
	}()

	query := "CREATE TABLE IF NOT EXISTS " + v.config.MigrationsTable + " (version bigint not null primary key, dirty boolean not null)"
	if _, err := v.conn.ExecContext(context.Background(), query); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}
	return nil
}

func (v *Vertica) configuredSchema() (string, error) {

	if v.config.Schema != "" {
		return v.config.Schema, nil
	}

	var schema string
	sql := `select CURRENT_SCHEMA()`
	rows, e := v.conn.QueryContext(context.Background(), sql)
	if e != nil {
		return "", e
	}
	rows.Scan(&schema)
	return schema, e
}
