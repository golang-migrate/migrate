package trino

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/golang-migrate/migrate/v4/database"
	"github.com/hashicorp/go-multierror"
	_ "github.com/trinodb/trino-go-client/trino" // Trino driver
)

func init() {
	database.Register("trino", &Trino{})
}

var (
	DefaultMigrationsTable = "schema_migrations"
	DefaultLockMethod      = "file"
	DefaultLockTimeout     = 15 * time.Minute
)

var (
	ErrNilConfig         = fmt.Errorf("no config")
	ErrNoCatalog         = fmt.Errorf("no catalog specified")
	ErrNoSchema          = fmt.Errorf("no schema specified")
	ErrInvalidLockMethod = fmt.Errorf("invalid lock method, must be 'file', 'table', or 'none'")
	ErrLockTimeout       = fmt.Errorf("lock timeout exceeded")
)

// Config holds the configuration for the Trino driver
type Config struct {
	MigrationsTable   string
	MigrationsSchema  string
	MigrationsCatalog string
	User              string
	Source            string // Application identifier for Trino
	StatementTimeout  time.Duration
	ConnectionTimeout time.Duration
	LockMethod        string // "file", "table", "none"
	LockFilePath      string
	LockTimeout       time.Duration
}

// Trino implements the database.Driver interface for Trino
type Trino struct {
	conn     *sql.Conn
	db       *sql.DB
	isLocked atomic.Bool
	config   *Config
	lockFile *os.File
}

// WithInstance creates a new Trino driver instance with an existing database connection
func WithInstance(instance *sql.DB, config *Config) (database.Driver, error) {
	if config == nil {
		return nil, ErrNilConfig
	}

	if err := instance.Ping(); err != nil {
		return nil, err
	}

	ctx := context.Background()
	conn, err := instance.Conn(ctx)
	if err != nil {
		return nil, err
	}

	t, err := WithConnection(ctx, conn, config)
	if err != nil {
		return nil, err
	}
	t.db = instance
	return t, nil
}

// WithConnection creates a new Trino driver instance with an existing connection
func WithConnection(ctx context.Context, conn *sql.Conn, config *Config) (*Trino, error) {
	if config == nil {
		return nil, ErrNilConfig
	}

	if err := conn.PingContext(ctx); err != nil {
		return nil, err
	}

	// Set defaults
	if config.MigrationsTable == "" {
		config.MigrationsTable = DefaultMigrationsTable
	}
	if config.LockMethod == "" {
		config.LockMethod = DefaultLockMethod
	}
	if config.LockTimeout == 0 {
		config.LockTimeout = DefaultLockTimeout
	}
	if config.Source == "" {
		config.Source = "golang-migrate"
	}

	// Validate lock method
	if config.LockMethod != "file" && config.LockMethod != "table" && config.LockMethod != "none" {
		return nil, ErrInvalidLockMethod
	}

	// Get current catalog and schema if not specified
	if config.MigrationsCatalog == "" {
		var catalog sql.NullString
		query := "SELECT current_catalog"
		if err := conn.QueryRowContext(ctx, query).Scan(&catalog); err != nil {
			return nil, &database.Error{OrigErr: err, Query: []byte(query)}
		}
		if !catalog.Valid || catalog.String == "" {
			return nil, ErrNoCatalog
		}
		config.MigrationsCatalog = catalog.String
	}

	if config.MigrationsSchema == "" {
		var schema sql.NullString
		query := "SELECT current_schema"
		if err := conn.QueryRowContext(ctx, query).Scan(&schema); err != nil {
			return nil, &database.Error{OrigErr: err, Query: []byte(query)}
		}
		if !schema.Valid || schema.String == "" {
			return nil, ErrNoSchema
		}
		config.MigrationsSchema = schema.String
	}

	t := &Trino{
		conn:   conn,
		config: config,
	}

	if err := t.ensureVersionTable(); err != nil {
		return nil, err
	}

	return t, nil
}

// Open creates a new Trino driver instance from a URL
func (t *Trino) Open(dsn string) (database.Driver, error) {
	purl, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}

	// Build Trino DSN from our custom URL format
	// Convert from: trino://user@host:port/catalog/schema?params
	// To Trino format: http://user@host:port?catalog=catalog&schema=schema&params

	trinoURL := &url.URL{
		Scheme: "http",
		Host:   purl.Host,
		User:   purl.User,
	}

	// Parse catalog and schema from path
	var catalog, schema string
	pathParts := strings.Split(strings.Trim(purl.Path, "/"), "/")
	if len(pathParts) >= 1 && pathParts[0] != "" {
		catalog = pathParts[0]
	}
	if len(pathParts) >= 2 && pathParts[1] != "" {
		schema = pathParts[1]
	}

	// Parse query parameters
	qv := purl.Query()

	// Build new query parameters for Trino driver
	trinoQuery := url.Values{}

	// Set catalog and schema
	if catalog != "" {
		trinoQuery.Set("catalog", catalog)
	}
	if schema != "" {
		trinoQuery.Set("schema", schema)
	}

	// Set source (required by Trino)
	if source := qv.Get("source"); source != "" {
		trinoQuery.Set("source", source)
	} else {
		trinoQuery.Set("source", "golang-migrate")
	}

	// Copy non-migration specific parameters to Trino driver
	for key, values := range qv {
		if !strings.HasPrefix(key, "x-") && key != "source" {
			for _, value := range values {
				trinoQuery.Add(key, value)
			}
		}
	}

	trinoURL.RawQuery = trinoQuery.Encode()

	// Handle custom migration parameters
	migrationConfig := &Config{
		MigrationsTable:   qv.Get("x-migrations-table"),
		MigrationsSchema:  qv.Get("x-migrations-schema"),
		MigrationsCatalog: qv.Get("x-migrations-catalog"),
		LockMethod:        qv.Get("x-lock-method"),
		LockFilePath:      qv.Get("x-lock-file-path"),
	}

	if purl.User != nil {
		migrationConfig.User = purl.User.Username()
	}

	migrationConfig.Source = trinoQuery.Get("source")

	// Parse timeouts
	if timeoutStr := qv.Get("x-statement-timeout"); timeoutStr != "" {
		if timeoutMs, err := strconv.Atoi(timeoutStr); err == nil {
			migrationConfig.StatementTimeout = time.Duration(timeoutMs) * time.Millisecond
		}
	}

	if timeoutStr := qv.Get("x-connection-timeout"); timeoutStr != "" {
		if timeoutMs, err := strconv.Atoi(timeoutStr); err == nil {
			migrationConfig.ConnectionTimeout = time.Duration(timeoutMs) * time.Millisecond
		}
	}

	if timeoutStr := qv.Get("x-lock-timeout"); timeoutStr != "" {
		if timeoutMs, err := strconv.Atoi(timeoutStr); err == nil {
			migrationConfig.LockTimeout = time.Duration(timeoutMs) * time.Millisecond
		}
	}

	// Use defaults from catalog/schema if not specified in query
	if migrationConfig.MigrationsCatalog == "" {
		migrationConfig.MigrationsCatalog = catalog
	}
	if migrationConfig.MigrationsSchema == "" {
		migrationConfig.MigrationsSchema = schema
	}

	// Open database connection using Trino driver
	db, err := sql.Open("trino", trinoURL.String())
	if err != nil {
		return nil, err
	}

	if err := db.Ping(); err != nil {
		return nil, err
	}

	return WithInstance(db, migrationConfig)
}

// Close closes the database connection
func (t *Trino) Close() error {
	connErr := t.conn.Close()
	var dbErr error
	if t.db != nil {
		dbErr = t.db.Close()
	}

	// Clean up any lock files
	if t.lockFile != nil {
		t.lockFile.Close()
		os.Remove(t.lockFile.Name())
		t.lockFile = nil
	}

	if connErr != nil || dbErr != nil {
		return fmt.Errorf("conn: %v, db: %v", connErr, dbErr)
	}
	return nil
}

// Lock acquires a migration lock
func (t *Trino) Lock() error {
	return database.CasRestoreOnErr(&t.isLocked, false, true, database.ErrLocked, func() error {
		switch t.config.LockMethod {
		case "file":
			return t.lockWithFile()
		case "table":
			return t.lockWithTable()
		case "none":
			return nil
		default:
			return ErrInvalidLockMethod
		}
	})
}

// Unlock releases the migration lock
func (t *Trino) Unlock() error {
	return database.CasRestoreOnErr(&t.isLocked, true, false, database.ErrNotLocked, func() error {
		switch t.config.LockMethod {
		case "file":
			return t.unlockFile()
		case "table":
			return t.unlockTable()
		case "none":
			return nil
		default:
			return ErrInvalidLockMethod
		}
	})
}

// lockWithFile implements file-based locking
func (t *Trino) lockWithFile() error {
	lockPath := t.config.LockFilePath
	if lockPath == "" {
		lockPath = filepath.Join(os.TempDir(), fmt.Sprintf("trino-migrate-%s-%s-%s.lock",
			t.config.MigrationsCatalog, t.config.MigrationsSchema, t.config.MigrationsTable))
	}

	// Try to create the lock file exclusively
	lockFile, err := os.OpenFile(lockPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		if os.IsExist(err) {
			return database.ErrLocked
		}
		return err
	}

	// Write lock information
	lockInfo := fmt.Sprintf("pid=%d\ntime=%s\nuser=%s\nsource=%s\n",
		os.Getpid(), time.Now().Format(time.RFC3339), t.config.User, t.config.Source)

	if _, err := lockFile.WriteString(lockInfo); err != nil {
		lockFile.Close()
		os.Remove(lockPath)
		return err
	}

	t.lockFile = lockFile
	return nil
}

// unlockFile removes the file-based lock
func (t *Trino) unlockFile() error {
	if t.lockFile != nil {
		lockPath := t.lockFile.Name()
		t.lockFile.Close()
		t.lockFile = nil
		return os.Remove(lockPath)
	}
	return nil
}

// lockWithTable implements table-based locking
func (t *Trino) lockWithTable() error {
	lockTable := fmt.Sprintf("%s.%s.%s_lock",
		t.config.MigrationsCatalog, t.config.MigrationsSchema, t.config.MigrationsTable)

	// Create lock table if it doesn't exist
	createQuery := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id VARCHAR(255) PRIMARY KEY,
			locked_at TIMESTAMP,
			locked_by VARCHAR(255)
		)`, lockTable)

	if _, err := t.conn.ExecContext(context.Background(), createQuery); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(createQuery)}
	}

	// Try to acquire lock
	lockId := fmt.Sprintf("%s-%s", t.config.User, t.config.Source)
	insertQuery := fmt.Sprintf(`
		INSERT INTO %s (id, locked_at, locked_by) 
		VALUES (?, CURRENT_TIMESTAMP, ?)`, lockTable)

	if _, err := t.conn.ExecContext(context.Background(), insertQuery, "migration", lockId); err != nil {
		// Check if it's a constraint violation (lock already exists)
		if strings.Contains(strings.ToLower(err.Error()), "duplicate") ||
			strings.Contains(strings.ToLower(err.Error()), "constraint") {
			return database.ErrLocked
		}
		return &database.Error{OrigErr: err, Query: []byte(insertQuery)}
	}

	return nil
}

// unlockTable removes the table-based lock
func (t *Trino) unlockTable() error {
	lockTable := fmt.Sprintf("%s.%s.%s_lock",
		t.config.MigrationsCatalog, t.config.MigrationsSchema, t.config.MigrationsTable)

	deleteQuery := fmt.Sprintf(`DELETE FROM %s WHERE id = ?`, lockTable)
	if _, err := t.conn.ExecContext(context.Background(), deleteQuery, "migration"); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(deleteQuery)}
	}

	return nil
}

// Run executes a migration
func (t *Trino) Run(migration io.Reader) error {
	migr, err := io.ReadAll(migration)
	if err != nil {
		return err
	}

	ctx := context.Background()
	if t.config.StatementTimeout != 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, t.config.StatementTimeout)
		defer cancel()
	}

	query := string(migr)
	if strings.TrimSpace(query) == "" {
		return nil
	}

	if _, err := t.conn.ExecContext(ctx, query); err != nil {
		return &database.Error{OrigErr: err, Err: "migration failed", Query: migr}
	}

	return nil
}

// SetVersion sets the current migration version
func (t *Trino) SetVersion(version int, dirty bool) error {
	tx, err := t.conn.BeginTx(context.Background(), &sql.TxOptions{})
	if err != nil {
		return &database.Error{OrigErr: err, Err: "transaction start failed"}
	}

	migrationsTable := fmt.Sprintf("%s.%s.%s",
		t.config.MigrationsCatalog, t.config.MigrationsSchema, t.config.MigrationsTable)

	// Clear existing version
	deleteQuery := fmt.Sprintf("DELETE FROM %s", migrationsTable)
	if _, err := tx.Exec(deleteQuery); err != nil {
		if errRollback := tx.Rollback(); errRollback != nil {
			err = multierror.Append(err, errRollback)
		}
		return &database.Error{OrigErr: err, Query: []byte(deleteQuery)}
	}

	// Insert new version if needed
	if version >= 0 || (version == database.NilVersion && dirty) {
		insertQuery := fmt.Sprintf("INSERT INTO %s (version, dirty) VALUES (?, ?)", migrationsTable)
		if _, err := tx.Exec(insertQuery, version, dirty); err != nil {
			if errRollback := tx.Rollback(); errRollback != nil {
				err = multierror.Append(err, errRollback)
			}
			return &database.Error{OrigErr: err, Query: []byte(insertQuery)}
		}
	}

	if err := tx.Commit(); err != nil {
		return &database.Error{OrigErr: err, Err: "transaction commit failed"}
	}

	return nil
}

// Version returns the current migration version
func (t *Trino) Version() (version int, dirty bool, err error) {
	migrationsTable := fmt.Sprintf("%s.%s.%s",
		t.config.MigrationsCatalog, t.config.MigrationsSchema, t.config.MigrationsTable)

	query := fmt.Sprintf("SELECT version, dirty FROM %s LIMIT 1", migrationsTable)
	err = t.conn.QueryRowContext(context.Background(), query).Scan(&version, &dirty)

	switch {
	case err == sql.ErrNoRows:
		return database.NilVersion, false, nil
	case err != nil:
		// Check if table doesn't exist
		if strings.Contains(strings.ToLower(err.Error()), "not exist") ||
			strings.Contains(strings.ToLower(err.Error()), "not found") {
			return database.NilVersion, false, nil
		}
		return 0, false, &database.Error{OrigErr: err, Query: []byte(query)}
	default:
		return version, dirty, nil
	}
}

// Drop removes all tables from the current schema
func (t *Trino) Drop() (err error) {
	// Get all tables in the current schema
	query := fmt.Sprintf(`
		SELECT table_name 
		FROM information_schema.tables 
		WHERE table_catalog = '%s' 
		AND table_schema = '%s' 
		AND table_type = 'BASE TABLE'`,
		t.config.MigrationsCatalog, t.config.MigrationsSchema)

	tables, err := t.conn.QueryContext(context.Background(), query)
	if err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}
	defer func() {
		if errClose := tables.Close(); errClose != nil {
			err = multierror.Append(err, errClose)
		}
	}()

	// Collect table names
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

	// Drop tables one by one
	for _, tableName := range tableNames {
		dropQuery := fmt.Sprintf("DROP TABLE IF EXISTS %s.%s.%s",
			t.config.MigrationsCatalog, t.config.MigrationsSchema, tableName)
		if _, err := t.conn.ExecContext(context.Background(), dropQuery); err != nil {
			return &database.Error{OrigErr: err, Query: []byte(dropQuery)}
		}
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

	// Check if table exists
	checkQuery := fmt.Sprintf(`
		SELECT COUNT(*) 
		FROM information_schema.tables 
		WHERE table_catalog = '%s' 
		AND table_schema = '%s' 
		AND table_name = '%s'`,
		t.config.MigrationsCatalog, t.config.MigrationsSchema, t.config.MigrationsTable)

	var count int
	if err := t.conn.QueryRowContext(context.Background(), checkQuery).Scan(&count); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(checkQuery)}
	}

	if count > 0 {
		return nil // Table already exists
	}

	// Create the migrations table
	createQuery := fmt.Sprintf(`
		CREATE TABLE %s (
			version BIGINT NOT NULL,
			dirty BOOLEAN NOT NULL
		)`, migrationsTable)

	if _, err := t.conn.ExecContext(context.Background(), createQuery); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(createQuery)}
	}

	return nil
}
