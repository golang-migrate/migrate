package trino

import (
	"database/sql"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
	"github.com/hashicorp/go-multierror"
	_ "github.com/trinodb/trino-go-client/trino"
)

var (
	DefaultMigrationsTable = "schema_migrations"
	ErrNilConfig           = fmt.Errorf("no config")
)

type Config struct {
	MigrationsTable   string
	MigrationsSchema  string
	MigrationsCatalog string
	StatementTimeout  time.Duration
}

func init() {
	database.Register("trino", &Trino{})
}

func WithInstance(conn *sql.DB, config *Config) (database.Driver, error) {
	if config == nil {
		return nil, ErrNilConfig
	}

	if err := conn.Ping(); err != nil {
		return nil, err
	}

	t := &Trino{
		conn:   conn,
		config: config,
	}

	if err := t.init(); err != nil {
		return nil, err
	}

	return t, nil
}

type Trino struct {
	conn     *sql.DB
	config   *Config
	isLocked atomic.Bool
}

func (t *Trino) Open(dsn string) (database.Driver, error) {
	purl, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}

	// Use Trino HTTP URL directly - just filter our custom parameters
	q := migrate.FilterCustomQuery(purl)

	// Set source if not provided
	query := q.Query()
	if query.Get("source") == "" {
		query.Set("source", "golang-migrate")
	}
	q.RawQuery = query.Encode()

	conn, err := sql.Open("trino", q.String())
	if err != nil {
		return nil, err
	}

	// Parse statement timeout
	var statementTimeout time.Duration
	if timeoutStr := purl.Query().Get("x-statement-timeout"); timeoutStr != "" {
		if timeoutMs, err := strconv.Atoi(timeoutStr); err == nil {
			statementTimeout = time.Duration(timeoutMs) * time.Millisecond
		}
	}

	t = &Trino{
		conn: conn,
		config: &Config{
			MigrationsTable:   purl.Query().Get("x-migrations-table"),
			MigrationsSchema:  purl.Query().Get("x-migrations-schema"),
			MigrationsCatalog: purl.Query().Get("x-migrations-catalog"),
			StatementTimeout:  statementTimeout,
		},
	}

	if err := t.init(); err != nil {
		return nil, err
	}

	return t, nil
}

func (t *Trino) init() error {
	// Test basic connectivity first
	if err := t.conn.Ping(); err != nil {
		return fmt.Errorf("ping failed: %w", err)
	}

	// Get current catalog if not specified
	if t.config.MigrationsCatalog == "" {
		if err := t.conn.QueryRow("SELECT current_catalog").Scan(&t.config.MigrationsCatalog); err != nil {
			return fmt.Errorf("failed to get current catalog: %w", err)
		}
	}

	// Get current schema if not specified
	if t.config.MigrationsSchema == "" {
		if err := t.conn.QueryRow("SELECT current_schema").Scan(&t.config.MigrationsSchema); err != nil {
			return fmt.Errorf("failed to get current schema: %w", err)
		}
	}

	if t.config.MigrationsTable == "" {
		t.config.MigrationsTable = DefaultMigrationsTable
	}

	return t.ensureVersionTable()
}

func (t *Trino) Run(r io.Reader) error {
	migration, err := io.ReadAll(r)
	if err != nil {
		return err
	}

	query := string(migration)
	if strings.TrimSpace(query) == "" {
		return nil
	}

	if _, err := t.conn.Exec(query); err != nil {
		return database.Error{OrigErr: err, Err: "migration failed", Query: migration}
	}

	return nil
}

func (t *Trino) Version() (int, bool, error) {
	var (
		version int
		dirty   bool
		query   = fmt.Sprintf("SELECT version, dirty FROM %s.%s.%s  ORDER BY sequence DESC LIMIT 1",
			t.config.MigrationsCatalog, t.config.MigrationsSchema, t.config.MigrationsTable)
	)

	err := t.conn.QueryRow(query).Scan(&version, &dirty)
	if err != nil {
		if err == sql.ErrNoRows {
			return database.NilVersion, false, nil
		}
		// Check if table doesn't exist
		if strings.Contains(strings.ToLower(err.Error()), "not exist") ||
			strings.Contains(strings.ToLower(err.Error()), "not found") {
			return database.NilVersion, false, nil
		}
		return 0, false, &database.Error{OrigErr: err, Query: []byte(query)}
	}
	return version, dirty, nil
}

func (t *Trino) SetVersion(version int, dirty bool) error {
	migrationsTable := fmt.Sprintf("%s.%s.%s",
		t.config.MigrationsCatalog, t.config.MigrationsSchema, t.config.MigrationsTable)

	insertQuery := fmt.Sprintf("INSERT INTO %s (version, dirty, sequence) VALUES (?, ?, ?)", migrationsTable)
	if _, err := t.conn.Exec(insertQuery, version, dirty, time.Now().UnixNano()); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(insertQuery)}
	}

	return nil
}

// ensureVersionTable creates the migrations table if it doesn't exist
func (t *Trino) ensureVersionTable() (err error) {
	if err = t.Lock(); err != nil {
		return err
	}

	defer func() {
		if e := t.Unlock(); e != nil {
			if err == nil {
				err = e
			} else {
				err = multierror.Append(err, e)
			}
		}
	}()

	migrationsTable := fmt.Sprintf("%s.%s.%s",
		t.config.MigrationsCatalog, t.config.MigrationsSchema, t.config.MigrationsTable)

	// Use CREATE TABLE IF NOT EXISTS for safe concurrent table creation
	createQuery := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			version BIGINT NOT NULL,
			dirty BOOLEAN NOT NULL,
			sequence BIGINT NOT NULL
		)`, migrationsTable)

	if _, err := t.conn.Exec(createQuery); err != nil {
		// Check if it's a "table already exists" error, which is safe to ignore
		if strings.Contains(strings.ToLower(err.Error()), "already exists") ||
			strings.Contains(strings.ToLower(err.Error()), "table exists") {
			return nil
		}
		return &database.Error{OrigErr: err, Query: []byte(createQuery)}
	}

	return nil
}

func (t *Trino) Drop() (err error) {
	// Get all tables in the current schema
	query := fmt.Sprintf(`
		SELECT table_name 
		FROM information_schema.tables 
		WHERE table_catalog = '%s' 
		AND table_schema = '%s' 
		AND table_type = 'BASE TABLE'`,
		t.config.MigrationsCatalog, t.config.MigrationsSchema)

	tables, err := t.conn.Query(query)
	if err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}
	defer func() {
		if errClose := tables.Close(); errClose != nil {
			err = multierror.Append(err, errClose)
		}
	}()

	// Drop tables one by one
	for tables.Next() {
		var tableName string
		if err := tables.Scan(&tableName); err != nil {
			return err
		}

		dropQuery := fmt.Sprintf("DROP TABLE IF EXISTS %s.%s.%s",
			t.config.MigrationsCatalog, t.config.MigrationsSchema, tableName)
		if _, err := t.conn.Exec(dropQuery); err != nil {
			return &database.Error{OrigErr: err, Query: []byte(dropQuery)}
		}
	}
	if err := tables.Err(); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}

	return nil
}

func (t *Trino) Lock() error {
	if !t.isLocked.CompareAndSwap(false, true) {
		return database.ErrLocked
	}
	return nil
}

func (t *Trino) Unlock() error {
	if !t.isLocked.CompareAndSwap(true, false) {
		return database.ErrNotLocked
	}
	return nil
}

func (t *Trino) Close() error {
	return t.conn.Close()
}
