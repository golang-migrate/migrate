//go:build go1.9

package ydb

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	nurl "net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
	"github.com/golang-migrate/migrate/v4/database/multistmt"
	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
)

func init() {
	db := YDB{}
	database.Register("grpc", &db)
	database.Register("grpcs", &db)
}

var (
	multiStmtDelimiter = []byte(";")

	DefaultMigrationsTable       = "schema_migrations"
	DefaultMultiStatementMaxSize = 10 * 1 << 20 // 10 MB
)

var (
	ErrNilConfig      = fmt.Errorf("no config")
	ErrNoDatabaseName = fmt.Errorf("no database name")
)

type Config struct {
	MigrationsTable       string
	DatabaseName          string
	MultiStatementEnabled bool
	MultiStatementMaxSize int
	StatementTimeout      time.Duration
}

type YDB struct {
	db       *sql.DB
	isLocked atomic.Bool

	// Open and WithInstance need to guarantee that config is never nil
	config *Config
}

func WithInstance(instance *sql.DB, config *Config) (database.Driver, error) {
	if config == nil {
		return nil, ErrNilConfig
	}

	if err := instance.Ping(); err != nil {
		return nil, err
	}

	if config.DatabaseName == "" {
		return nil, ErrNoDatabaseName
	}

	if len(config.MigrationsTable) == 0 {
		config.MigrationsTable = DefaultMigrationsTable
	}

	if config.MultiStatementMaxSize <= 0 {
		config.MultiStatementMaxSize = DefaultMultiStatementMaxSize
	}

	ydb := &YDB{
		db:     instance,
		config: config,
	}

	if err := ydb.ensureVersionTable(); err != nil {
		return nil, err
	}

	return ydb, nil
}

func (y *YDB) Open(url string) (database.Driver, error) {
	purl, err := nurl.Parse(url)
	if err != nil {
		return nil, err
	}

	// Build the YDB connection string: grpc[s]://host:port/dbname
	// Strip custom x-* query params before passing to sql.Open
	dbURL := migrate.FilterCustomQuery(purl).String()

	db, err := sql.Open("ydb", dbURL)
	if err != nil {
		return nil, err
	}

	migrationsTable := purl.Query().Get("x-migrations-table")

	statementTimeoutString := purl.Query().Get("x-statement-timeout")
	statementTimeout := 0
	if statementTimeoutString != "" {
		statementTimeout, err = strconv.Atoi(statementTimeoutString)
		if err != nil {
			return nil, err
		}
	}

	multiStatementMaxSize := DefaultMultiStatementMaxSize
	if s := purl.Query().Get("x-multi-statement-max-size"); len(s) > 0 {
		multiStatementMaxSize, err = strconv.Atoi(s)
		if err != nil {
			return nil, err
		}
		if multiStatementMaxSize <= 0 {
			multiStatementMaxSize = DefaultMultiStatementMaxSize
		}
	}

	multiStatementEnabled := false
	if s := purl.Query().Get("x-multi-statement"); len(s) > 0 {
		multiStatementEnabled, err = strconv.ParseBool(s)
		if err != nil {
			return nil, fmt.Errorf("unable to parse option x-multi-statement: %w", err)
		}
	}

	// Extract database name from URL path
	dbName := purl.Path
	if dbName == "" {
		dbName = "/"
	}

	ydbDriver, err := WithInstance(db, &Config{
		DatabaseName:          dbName,
		MigrationsTable:       migrationsTable,
		StatementTimeout:      time.Duration(statementTimeout) * time.Millisecond,
		MultiStatementEnabled: multiStatementEnabled,
		MultiStatementMaxSize: multiStatementMaxSize,
	})
	if err != nil {
		if closeErr := db.Close(); closeErr != nil {
			return nil, fmt.Errorf("failed to close db after WithInstance error: %v, original: %w", closeErr, err)
		}
		return nil, err
	}

	return ydbDriver, nil
}

func (y *YDB) Close() error {
	if y.db != nil {
		return y.db.Close()
	}
	return nil
}

// Lock implements database.Driver. YDB does not support advisory locks,
// so we use an in-process atomic bool to prevent concurrent migrations.
func (y *YDB) Lock() error {
	return database.CasRestoreOnErr(&y.isLocked, false, true, database.ErrLocked, func() error {
		return nil
	})
}

// Unlock implements database.Driver.
func (y *YDB) Unlock() error {
	return database.CasRestoreOnErr(&y.isLocked, true, false, database.ErrNotLocked, func() error {
		return nil
	})
}

func (y *YDB) Run(migration io.Reader) error {
	if y.config.MultiStatementEnabled {
		var err error
		if e := multistmt.Parse(migration, multiStmtDelimiter, y.config.MultiStatementMaxSize, func(m []byte) bool {
			if err = y.runStatement(m); err != nil {
				return false
			}
			return true
		}); e != nil {
			return e
		}
		return err
	}
	migr, err := io.ReadAll(migration)
	if err != nil {
		return err
	}
	return y.runStatement(migr)
}

func (y *YDB) runStatement(statement []byte) error {
	ctx := context.Background()
	if y.config.StatementTimeout != 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, y.config.StatementTimeout)
		defer cancel()
	}
	query := string(statement)
	if strings.TrimSpace(query) == "" {
		return nil
	}
	// DDL statements in YDB require SchemeQueryMode
	if isDDL(query) {
		ctx = ydb.WithQueryMode(ctx, ydb.SchemeQueryMode)
	}
	if _, err := y.db.ExecContext(ctx, query); err != nil {
		return database.Error{OrigErr: err, Err: "migration failed", Query: statement}
	}
	return nil
}

// isDDL checks if a query is a DDL statement that requires SchemeQueryMode in YDB.
// It strips leading SQL comments (-- and /* */) before checking.
func isDDL(query string) bool {
	q := strings.TrimSpace(query)
	// Strip leading single-line comments
	for strings.HasPrefix(q, "--") {
		if idx := strings.Index(q, "\n"); idx >= 0 {
			q = strings.TrimSpace(q[idx+1:])
		} else {
			// entire query is a comment
			return false
		}
	}
	// Strip leading block comments
	for strings.HasPrefix(q, "/*") {
		if idx := strings.Index(q, "*/"); idx >= 0 {
			q = strings.TrimSpace(q[idx+2:])
		} else {
			// unclosed block comment
			return false
		}
	}
	q = strings.ToUpper(q)
	return strings.HasPrefix(q, "CREATE ") ||
		strings.HasPrefix(q, "DROP ") ||
		strings.HasPrefix(q, "ALTER ")
}

func (y *YDB) SetVersion(version int, dirty bool) error {
	ctx := context.Background()

	tx, err := y.db.BeginTx(ctx, nil)
	if err != nil {
		return &database.Error{OrigErr: err, Err: "transaction start failed"}
	}

	// Delete all rows from the migrations table
	query := "DELETE FROM `" + y.config.MigrationsTable + "`"
	if _, err := tx.ExecContext(ctx, query); err != nil {
		if errRollback := tx.Rollback(); errRollback != nil {
			err = fmt.Errorf("%w: %v", err, errRollback)
		}
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}

	// Also re-write the schema version for nil dirty versions to prevent
	// empty schema version for failed down migration on the first migration
	// See: https://github.com/golang-migrate/migrate/issues/330
	if version >= 0 || (version == database.NilVersion && dirty) {
		query = "UPSERT INTO `" + y.config.MigrationsTable + "` (version, dirty) VALUES (" +
			strconv.Itoa(version) + ", " + strconv.FormatBool(dirty) + ")"
		if _, err := tx.ExecContext(ctx, query); err != nil {
			if errRollback := tx.Rollback(); errRollback != nil {
				err = fmt.Errorf("%w: %v", err, errRollback)
			}
			return &database.Error{OrigErr: err, Query: []byte(query)}
		}
	}

	if err := tx.Commit(); err != nil {
		return &database.Error{OrigErr: err, Err: "transaction commit failed"}
	}

	return nil
}

func (y *YDB) Version() (version int, dirty bool, err error) {
	query := "SELECT version, dirty FROM `" + y.config.MigrationsTable + "` LIMIT 1"
	err = y.db.QueryRowContext(context.Background(), query).Scan(&version, &dirty)
	switch {
	case err == sql.ErrNoRows:
		return database.NilVersion, false, nil

	case err != nil:
		// If the table doesn't exist, return NilVersion
		if isTableNotExistsError(err) {
			return database.NilVersion, false, nil
		}
		return 0, false, &database.Error{OrigErr: err, Query: []byte(query)}

	default:
		return version, dirty, nil
	}
}

func (y *YDB) Drop() (err error) {
	ctx := context.Background()

	// YDB: list all tables via a scheme query
	query := "SELECT Path FROM `.sys/partition_stats`"
	rows, err := y.db.QueryContext(ctx, query)
	if err != nil {
		// If the system table is not accessible, try a simpler approach
		// Just try to drop the migrations table
		schemeCtx := ydb.WithQueryMode(ctx, ydb.SchemeQueryMode)
		dropQuery := "DROP TABLE `" + y.config.MigrationsTable + "`"
		if _, dropErr := y.db.ExecContext(schemeCtx, dropQuery); dropErr != nil {
			return &database.Error{OrigErr: err, Query: []byte(query)}
		}
		return nil
	}
	defer func() {
		if errClose := rows.Close(); errClose != nil {
			if err == nil {
				err = errClose
			}
		}
	}()

	tableNames := make(map[string]struct{})
	for rows.Next() {
		var tablePath string
		if err := rows.Scan(&tablePath); err != nil {
			return err
		}
		if len(tablePath) > 0 {
			// Extract just the table name from the full path
			parts := strings.Split(tablePath, "/")
			tableName := parts[len(parts)-1]
			// Skip system directories/tables
			if strings.HasPrefix(tableName, ".sys") {
				continue
			}
			tableNames[tableName] = struct{}{}
		}
	}
	if err := rows.Err(); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}

	// Drop each table using SchemeQueryMode
	schemeCtx := ydb.WithQueryMode(ctx, ydb.SchemeQueryMode)
	for tableName := range tableNames {
		dropQuery := "DROP TABLE `" + tableName + "`"
		if _, err := y.db.ExecContext(schemeCtx, dropQuery); err != nil {
			return &database.Error{OrigErr: err, Query: []byte(dropQuery)}
		}
	}

	return nil
}

// ensureVersionTable checks if versions table exists and, if not, creates it.
// Note that this function locks the database, which deviates from the usual
// convention of "caller locks" in the YDB type.
func (y *YDB) ensureVersionTable() (err error) {
	if err = y.Lock(); err != nil {
		return err
	}

	defer func() {
		if e := y.Unlock(); e != nil {
			if err == nil {
				err = e
			} else {
				err = fmt.Errorf("%w: %v", err, e)
			}
		}
	}()

	// Check if the table already exists by trying to query it
	query := "SELECT version, dirty FROM `" + y.config.MigrationsTable + "` LIMIT 1"
	rows, scanErr := y.db.QueryContext(context.Background(), query)
	if scanErr == nil {
		rows.Close()
		// Table exists
		return nil
	}

	// If the table doesn't exist, create it
	if isTableNotExistsError(scanErr) {
		createQuery := "CREATE TABLE `" + y.config.MigrationsTable + "` (" +
			"version Int64 NOT NULL, " +
			"dirty Bool NOT NULL, " +
			"PRIMARY KEY (version)" +
			")"
		// DDL operations in YDB require SchemeQueryMode
		ctx := ydb.WithQueryMode(context.Background(), ydb.SchemeQueryMode)
		if _, err = y.db.ExecContext(ctx, createQuery); err != nil {
			return &database.Error{OrigErr: err, Query: []byte(createQuery)}
		}
		return nil
	}

	return &database.Error{OrigErr: scanErr, Query: []byte(query)}
}

// isTableNotExistsError checks if an error indicates that a table does not exist in YDB.
func isTableNotExistsError(err error) bool {
	if err == nil {
		return false
	}
	// Use YDB SDK error helper for "not found" errors specifically
	if ydb.IsOperationErrorNotFoundError(err) {
		return true
	}
	// Fallback to string matching for errors that may not be wrapped properly
	errStr := err.Error()
	return strings.Contains(errStr, "path not found") ||
		strings.Contains(errStr, "Path not found") ||
		strings.Contains(errStr, "scheme error: path not found")
}
