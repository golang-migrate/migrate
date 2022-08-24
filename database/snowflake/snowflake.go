package snowflake

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	nurl "net/url"
	"strconv"
	"strings"
	"time"

	"github.com/golang-migrate/migrate/v4/database"
	"github.com/hashicorp/go-multierror"
	sf "github.com/snowflakedb/gosnowflake"
	"go.uber.org/atomic"
)

func init() {
	db := Snowflake{}
	database.Register("snowflake", &db)
}

const (
	DefaultMigrationsTable = "schema_migrations"
	DefaultRequestTimeout  = 5 * time.Minute
	DefaultConnectTimeout  = 30 * time.Second
)

var (
	ErrNilConfig              = fmt.Errorf("no config")
	ErrNoDatabaseName         = fmt.Errorf("no database name")
	ErrNoPassword             = fmt.Errorf("no password")
	ErrNoSchema               = fmt.Errorf("no schema")
	ErrNoSchemaOrDatabase     = fmt.Errorf("no schema/database name")
	ErrInvalidParameterFormat = fmt.Errorf("invalid parameter format")
)

type Config struct {
	MigrationsTable       string
	DatabaseName          string
	MultiStatementEnabled bool
	ConnectTimeout        time.Duration
	dsn                   string
}

type Snowflake struct {
	isLocked atomic.Bool
	conn     *sql.Conn
	db       *sql.DB

	// Open and WithInstance need to guarantee that config is never nil
	config *Config
}

func WithInstance(instance *sql.DB, config *Config) (database.Driver, error) {
	if config == nil {
		return nil, ErrNilConfig
	}
	ctx, cancel := context.WithTimeout(context.Background(), config.ConnectTimeout)
	defer cancel()

	if err := instance.PingContext(ctx); err != nil {
		return nil, err
	}

	if config.DatabaseName == "" {
		query := `SELECT CURRENT_DATABASE()`
		var databaseName string
		if err := instance.QueryRow(query).Scan(&databaseName); err != nil {
			return nil, &database.Error{OrigErr: err, Query: []byte(query)}
		}

		if len(databaseName) == 0 {
			return nil, ErrNoDatabaseName
		}

		config.DatabaseName = databaseName
	}

	if len(config.MigrationsTable) == 0 {
		config.MigrationsTable = DefaultMigrationsTable
	}

	conn, err := instance.Conn(ctx)

	if err != nil {
		return nil, err
	}

	px := &Snowflake{
		conn:   conn,
		db:     instance,
		config: config,
	}

	if err := px.ensureVersionTable(); err != nil {
		return nil, err
	}

	return px, nil
}

func configForURL(url string) (*Config, error) {
	purl, err := nurl.Parse(url)
	if err != nil {
		return nil, err
	}

	password, isPasswordSet := purl.User.Password()
	if !isPasswordSet {
		return nil, ErrNoPassword
	}

	splitPath := strings.Split(purl.Path, "/")
	if len(splitPath) < 3 {
		return nil, ErrNoSchemaOrDatabase
	}

	database := splitPath[2]
	if len(database) == 0 {
		return nil, ErrNoDatabaseName
	}

	schema := splitPath[1]
	if len(schema) == 0 {
		return nil, ErrNoSchema
	}

	cfg := &sf.Config{
		Account:        purl.Host,
		User:           purl.User.Username(),
		Password:       password,
		Database:       database,
		Schema:         schema,
		RequestTimeout: DefaultRequestTimeout,
	}

	if warehouse := purl.Query().Get("x-warehouse"); len(warehouse) > 0 {
		cfg.Warehouse = warehouse
	}
	if role := purl.Query().Get("x-role"); len(role) > 0 {
		cfg.Role = role
	}
	if timeout := purl.Query().Get("x-timeout"); len(timeout) > 0 {
		timeoutSeconds, err := strconv.ParseInt(timeout, 10, 64)
		if err != nil {
			return nil, ErrInvalidParameterFormat
		}
		cfg.RequestTimeout = time.Duration(timeoutSeconds) * time.Second
	}

	dsn, err := sf.DSN(cfg)
	if err != nil {
		return nil, err
	}

	migrationsTable := purl.Query().Get("x-migrations-table")

	multiStatement := false
	if multi := purl.Query().Get("x-multi-statement"); len(multi) > 0 {
		multiStatement, err = strconv.ParseBool(multi)
		if err != nil {
			return nil, ErrInvalidParameterFormat
		}
	}

	config := &Config{
		DatabaseName:          database,
		MigrationsTable:       migrationsTable,
		MultiStatementEnabled: multiStatement,
		ConnectTimeout:        DefaultConnectTimeout,
		dsn:                   dsn,
	}

	if connectTimeout := purl.Query().Get("x-connect-timeout"); len(connectTimeout) > 0 {
		connectTimeoutSeconds, err := strconv.ParseInt(connectTimeout, 10, 64)
		if err != nil {
			return nil, ErrInvalidParameterFormat
		}
		config.ConnectTimeout = time.Duration(connectTimeoutSeconds) * time.Second
	}

	return config, nil
}

func (p *Snowflake) Open(url string) (database.Driver, error) {
	cfg, err := configForURL(url)
	if err != nil {
		return nil, err
	}

	db, err := sql.Open("snowflake", cfg.dsn)
	if err != nil {
		return nil, err
	}

	px, err := WithInstance(db, cfg)
	if err != nil {
		return nil, err
	}

	return px, nil
}

func (p *Snowflake) Close() error {
	connErr := p.conn.Close()
	dbErr := p.db.Close()
	if connErr != nil || dbErr != nil {
		return fmt.Errorf("conn: %v, db: %v", connErr, dbErr)
	}
	return nil
}

func (p *Snowflake) Lock() error {
	if !p.isLocked.CAS(false, true) {
		return database.ErrLocked
	}
	return nil
}

func (p *Snowflake) Unlock() error {
	if !p.isLocked.CAS(true, false) {
		return database.ErrNotLocked
	}
	return nil
}

func (p *Snowflake) Run(migration io.Reader) error {
	migr, err := io.ReadAll(migration)
	if err != nil {
		return err
	}

	ctx := context.Background()
	if p.config.MultiStatementEnabled {
		// allow variable number of statements in the request by setting MULTI_STATEMENT_COUNT to 0
		// https://docs.snowflake.com/en/developer-guide/sql-api/submitting-multiple-statements.html#specifying-multiple-sql-statements-in-the-request
		if ctx, err = sf.WithMultiStatement(ctx, 0); err != nil {
			return err
		}
	}

	// run migration
	query := string(migr[:])
	if _, err := p.conn.ExecContext(ctx, query); err != nil {
		// gosnowflake.SnowflakeError does not return a line number, so there's no need to parse it
		return database.Error{OrigErr: err, Err: "migration failed", Query: migr}
	}

	return nil
}

func (p *Snowflake) SetVersion(version int, dirty bool) error {
	tx, err := p.conn.BeginTx(context.Background(), &sql.TxOptions{})
	if err != nil {
		return &database.Error{OrigErr: err, Err: "transaction start failed"}
	}

	query := `DELETE FROM "` + p.config.MigrationsTable + `"`
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
		query = `INSERT INTO "` + p.config.MigrationsTable + `" (version,
				dirty) VALUES (` + strconv.FormatInt(int64(version), 10) + `,
				` + strconv.FormatBool(dirty) + `)`
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

func (p *Snowflake) Version() (version int, dirty bool, err error) {
	query := `SELECT version, dirty FROM "` + p.config.MigrationsTable + `" LIMIT 1`
	err = p.conn.QueryRowContext(context.Background(), query).Scan(&version, &dirty)
	switch {
	case err == sql.ErrNoRows:
		return database.NilVersion, false, nil

	case err != nil:
		if e, ok := err.(*sf.SnowflakeError); ok {
			if e.Number == sf.ErrObjectNotExistOrAuthorized {
				return database.NilVersion, false, nil
			}
		}
		return 0, false, &database.Error{OrigErr: err, Query: []byte(query)}

	default:
		return version, dirty, nil
	}
}

func (p *Snowflake) Drop() (err error) {
	// select all tables in current schema
	query := `SELECT table_name FROM information_schema.tables WHERE table_schema=(SELECT current_schema()) AND table_type='BASE TABLE'`
	tables, err := p.conn.QueryContext(context.Background(), query)
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
	if err := tables.Err(); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}

	if len(tableNames) > 0 {
		// delete one by one ...
		for _, t := range tableNames {
			query = `DROP TABLE IF EXISTS ` + t + ` CASCADE`
			if _, err := p.conn.ExecContext(context.Background(), query); err != nil {
				return &database.Error{OrigErr: err, Query: []byte(query)}
			}
		}
	}

	return nil
}

// ensureVersionTable checks if versions table exists and, if not, creates it.
// Note that this function locks the database, which deviates from the usual
// convention of "caller locks" in the Snowflake type.
func (p *Snowflake) ensureVersionTable() (err error) {
	if err = p.Lock(); err != nil {
		return err
	}

	defer func() {
		if e := p.Unlock(); e != nil {
			if err == nil {
				err = e
			} else {
				err = multierror.Append(err, e)
			}
		}
	}()

	// check if migration table exists
	var count int
	query := `SELECT COUNT(1) FROM information_schema.tables WHERE table_name = $1 AND table_schema = (SELECT current_schema()) LIMIT 1`
	if err := p.conn.QueryRowContext(context.Background(), query, p.config.MigrationsTable).Scan(&count); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}
	if count == 1 {
		return nil
	}

	// if not, create the empty migration table
	query = `CREATE TABLE if not exists "` + p.config.MigrationsTable + `" (
			version bigint not null primary key, dirty boolean not null)`
	if _, err := p.conn.ExecContext(context.Background(), query); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}

	return nil
}
