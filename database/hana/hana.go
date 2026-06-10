// Package hana provides go-migrate driver for SAP HANA Cloud.
package hana

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	nurl "net/url"
	"strconv"
	"strings"
	"time"

	hdbDriver "github.com/SAP/go-hdb/driver"
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
	"github.com/golang-migrate/migrate/v4/database/multistmt"
)

func init() {
	database.Register("hdb", &Hana{})
}

// compile time interface compliance assertion
var _ database.Driver = (*Hana)(nil)

const maxMigrationSize = 10 * 1024 * 1024 // 10 MB

var (
	DefaultMigrationsTable         = "schema_migrations"
	DefaultLockName                = "migrate"
	DefaultMultiStatementDelimiter = ";"

	ErrNilConfig               = fmt.Errorf("no config")
	ErrNoSchemaName            = fmt.Errorf("no schema name")
	ErrInvalidStatementTimeout = fmt.Errorf("invalid x-statement-timeout")
	ErrInvalidIsolationLevel   = fmt.Errorf("invalid isolation level")
	ErrParseIsolationLevel     = fmt.Errorf("could not parse x-isolation-level")
	ErrParseLockTimeout        = fmt.Errorf("could not parse x-lock-timeout")
	ErrInvalidLockTimeout      = fmt.Errorf("invalid lock timeout")
	ErrSchemaMismatch          = fmt.Errorf("schema mismatch")
	ErrMigrationTableCount     = fmt.Errorf("version table count inconsistent")
)

type Config struct {
	SchemaName              string
	MigrationsTable         string
	StatementTimeout        time.Duration
	IsolationLevel          sql.IsolationLevel
	LockName                string
	LockTimeout             time.Duration // 0 = no wait, error immediately if already locked
	MultiStatementDelimiter string        // delimiter for splitting migration files into statements (default ";")
}

type Hana struct {
	db              *sql.DB
	config          *Config
	lockTransaction *sql.Tx
}

// Open opens the DB from driver string.
// Only validates parsing related errors.
// Further validations are done in WithInstance.
func (h *Hana) Open(url string) (database.Driver, error) {
	purl, err := nurl.Parse(url)
	if err != nil {
		return nil, err
	}

	schemaName := purl.Query().Get("x-migrations-schema")

	migrationsTable := purl.Query().Get("x-migrations-table")

	statementTimeoutParam := purl.Query().Get("x-statement-timeout")
	var statementTimeout time.Duration
	if statementTimeoutParam != "" {
		statementTimeout, err = time.ParseDuration(statementTimeoutParam)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrInvalidStatementTimeout, err)
		}
	}

	isolationLevel := sql.LevelDefault
	if isolationLevelParam := purl.Query().Get("x-isolation-level"); isolationLevelParam != "" {
		isolationLevelInt, err := strconv.Atoi(isolationLevelParam)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrParseIsolationLevel, err)
		}

		isolationLevel = sql.IsolationLevel(isolationLevelInt)
	}

	lockName := purl.Query().Get("x-lock-name")

	var lockTimeout time.Duration
	if lockTimeoutParam := purl.Query().Get("x-lock-timeout"); lockTimeoutParam != "" {
		lockTimeout, err = time.ParseDuration(lockTimeoutParam)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrParseLockTimeout, err)
		}
	}

	multiStatementDelimiter := purl.Query().Get("x-multi-statement-delimiter")

	// strip away custom parameters (x-)
	dsn := migrate.FilterCustomQuery(purl).String()
	connector, err := hdbDriver.NewDSNConnector(dsn)
	if err != nil {
		return nil, err
	}

	connector.SetDefaultSchema(schemaName)
	db := sql.OpenDB(connector)

	return WithInstance(db, &Config{
		MigrationsTable:         migrationsTable,
		SchemaName:              schemaName,
		StatementTimeout:        statementTimeout,
		IsolationLevel:          isolationLevel,
		LockName:                lockName,
		LockTimeout:             lockTimeout,
		MultiStatementDelimiter: multiStatementDelimiter,
	})
}

func (h *Hana) Close() error {
	return h.db.Close()
}

func (h *Hana) Lock() error {
	tx, err := h.beginTx()
	if err != nil {
		return &database.Error{OrigErr: err, Err: "failed to start lock transaction"}
	}

	ctx, cancel := contextWithTimeout(h.config.StatementTimeout)
	defer cancel()

	_, err = tx.ExecContext(ctx,
		"CALL ACQUIRE_APPLICATION_LOCK(?, 'EXCLUSIVE', 'TRANSACTION', ?)",
		h.config.LockName, h.config.LockTimeout.Milliseconds(),
	)
	if err != nil {
		rollbackErr := tx.Rollback()
		return errors.Join(rollbackErr, &database.Error{OrigErr: err, Err: "failed to acquire application lock"})
	}

	h.lockTransaction = tx
	return nil
}

func (h *Hana) Unlock() error {
	ctx, cancel := contextWithTimeout(h.config.StatementTimeout)
	defer cancel()

	_, err := h.lockTransaction.ExecContext(ctx,
		"CALL RELEASE_APPLICATION_LOCK(?, 'TRANSACTION')",
		h.config.LockName,
	)
	if err != nil {
		rollbackErr := h.lockTransaction.Rollback()
		h.lockTransaction = nil
		return errors.Join(rollbackErr, &database.Error{OrigErr: err, Err: "failed to release application lock"})
	}

	err = h.lockTransaction.Commit()
	if err != nil {
		h.lockTransaction = nil
		return &database.Error{OrigErr: err, Err: "failed to commit lock transaction"}
	}

	h.lockTransaction = nil
	return nil
}

func (h *Hana) Run(migration io.Reader) error {
	tx, err := h.beginTx()
	if err != nil {
		return &database.Error{OrigErr: err, Err: "failed to start migration transaction"}
	}

	var runErr error
	parseErr := multistmt.Parse(migration, []byte(h.config.MultiStatementDelimiter), maxMigrationSize, func(stmt []byte) bool {
		if runErr = h.runStatement(tx, stmt); runErr != nil {
			return false
		}
		return true
	})

	if parseErr != nil {
		rollbackErr := tx.Rollback()
		return errors.Join(rollbackErr, &database.Error{OrigErr: parseErr, Err: "failed to parse migration"})
	}

	if runErr != nil {
		rollbackErr := tx.Rollback()
		return errors.Join(rollbackErr, runErr)
	}

	err = tx.Commit()
	if err != nil {
		return &database.Error{OrigErr: err, Err: "failed to commit migration transaction"}
	}

	return nil
}

func (h *Hana) SetVersion(version int, dirty bool) error {
	tx, err := h.beginTx()
	if err != nil {
		return &database.Error{OrigErr: err, Err: "transaction start failed"}
	}

	query := fmt.Sprintf("DELETE FROM %s.%s", hdbDriver.Identifier(h.config.SchemaName), hdbDriver.Identifier(h.config.MigrationsTable))

	ctx, cancel := contextWithTimeout(h.config.StatementTimeout)
	defer cancel()

	_, err = tx.ExecContext(ctx, query)
	if err != nil {
		if errRollback := tx.Rollback(); errRollback != nil {
			err = errors.Join(err, errRollback)
		}

		return &database.Error{OrigErr: err, Query: []byte(query)}
	}

	// Also re-write the schema version for nil dirty versions to prevent
	// empty schema version for failed down migration on the first migration.
	// See: https://github.com/golang-migrate/migrate/issues/330
	if version >= 0 || (version == database.NilVersion && dirty) {
		query = fmt.Sprintf("INSERT INTO %s.%s (version, dirty) VALUES (?, ?)", hdbDriver.Identifier(h.config.SchemaName), hdbDriver.Identifier(h.config.MigrationsTable))

		ctx, cancel = contextWithTimeout(h.config.StatementTimeout)
		defer cancel()

		_, err = tx.ExecContext(ctx, query, version, dirty)
		if err != nil {
			if errRollback := tx.Rollback(); errRollback != nil {
				err = errors.Join(err, errRollback)
			}

			return &database.Error{OrigErr: err, Query: []byte(query)}
		}
	}

	err = tx.Commit()
	if err != nil {
		return &database.Error{OrigErr: err, Err: "transaction commit failed"}
	}

	return nil
}

func (h *Hana) Version() (version int, dirty bool, err error) {
	query := fmt.Sprintf("SELECT version, dirty FROM %s.%s",
		hdbDriver.Identifier(h.config.SchemaName), hdbDriver.Identifier(h.config.MigrationsTable))

	ctx, cancel := contextWithTimeout(h.config.StatementTimeout)
	defer cancel()

	rows, err := h.db.QueryContext(ctx, query)
	if err != nil {
		return 0, false, &database.Error{OrigErr: err, Query: []byte(query)}
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			err = errors.Join(err, closeErr)
		}
	}()

	noRow := !rows.Next()
	if noRow {
		err = rows.Err()
		if err != nil {
			return 0, false, &database.Error{OrigErr: err, Query: []byte(query)}
		}
		return database.NilVersion, false, nil
	}

	err = rows.Scan(&version, &dirty)
	if err != nil {
		return 0, false, &database.Error{OrigErr: err, Query: []byte(query)}
	}

	extraRow := rows.Next()
	if extraRow {
		return 0, false, &database.Error{Err: "expected 0 or 1 rows in migrations table, got more"}
	}

	return version, dirty, nil
}

func (h *Hana) Drop() (err error) {
	query := `SELECT TABLE_NAME FROM SYS.TABLES WHERE SCHEMA_NAME = ?`

	ctx, cancel := contextWithTimeout(h.config.StatementTimeout)
	defer cancel()

	tables, err := h.db.QueryContext(ctx, query, h.config.SchemaName)
	if err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}

	defer func() {
		if errClose := tables.Close(); errClose != nil {
			err = errors.Join(err, errClose)
		}
	}()

	tableNames := make([]string, 0)
	for tables.Next() {
		var tableName string
		err := tables.Scan(&tableName)
		if err != nil {
			return &database.Error{OrigErr: err, Query: []byte(query)}
		}

		if tableName != "" {
			tableNames = append(tableNames, tableName)
		}
	}

	err = tables.Err()
	if err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}

	for _, t := range tableNames {
		query = fmt.Sprintf("DROP TABLE %s.%s", hdbDriver.Identifier(h.config.SchemaName), hdbDriver.Identifier(t))

		ctx, cancel = contextWithTimeout(h.config.StatementTimeout)
		defer cancel()

		_, err = h.db.ExecContext(ctx, query)
		if err != nil {
			return &database.Error{OrigErr: err, Query: []byte(query)}
		}
	}

	return nil
}

// WithInstance returns a HANA migrate struct.
// Sanity checks config values.
func WithInstance(instance *sql.DB, config *Config) (database.Driver, error) {
	if config == nil {
		return nil, ErrNilConfig
	}

	if config.IsolationLevel < sql.LevelDefault || config.IsolationLevel > sql.LevelLinearizable {
		return nil, fmt.Errorf("%w: %d", ErrInvalidIsolationLevel, config.IsolationLevel)
	}

	if config.SchemaName == "" {
		return nil, ErrNoSchemaName
	}

	if config.LockTimeout < 0 {
		return nil, fmt.Errorf("%w: must not be negative", ErrInvalidLockTimeout)
	}

	ctx, cancel := contextWithTimeout(config.StatementTimeout)
	defer cancel()

	err := instance.PingContext(ctx)
	if err != nil {
		return nil, err
	}

	ctx, cancel = contextWithTimeout(config.StatementTimeout)
	defer cancel()

	var currentSchema string
	err = instance.QueryRowContext(ctx,
		"SELECT CURRENT_SCHEMA FROM DUMMY").Scan(&currentSchema)
	if err != nil {
		return nil, fmt.Errorf("failed to query current schema: %w", err)
	}
	if currentSchema != config.SchemaName {
		return nil, fmt.Errorf("%w: config schema %q does not match connection current schema %q",
			ErrSchemaMismatch, config.SchemaName, currentSchema)
	}

	hx := &Hana{
		db:     instance,
		config: config,
	}

	if config.MigrationsTable == "" {
		config.MigrationsTable = DefaultMigrationsTable
	}

	if config.LockName == "" {
		config.LockName = DefaultLockName
	}

	if config.MultiStatementDelimiter == "" {
		config.MultiStatementDelimiter = DefaultMultiStatementDelimiter
	}

	err = hx.ensureVersionTable()
	if err != nil {
		return nil, err
	}

	return hx, nil
}

// contextWithTimeout returns a context with the given timeout applied.
// If timeout is 0, returns a background context with a no-op cancel func.
func contextWithTimeout(timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout != 0 {
		return context.WithTimeout(context.Background(), timeout)
	}
	return context.Background(), func() {}
}

func (h *Hana) beginTx() (*sql.Tx, error) {
	return h.db.BeginTx(context.Background(), &sql.TxOptions{Isolation: h.config.IsolationLevel})
}

func (h *Hana) runStatement(tx *sql.Tx, stmt []byte) error {
	query := strings.TrimSpace(string(stmt))
	query = strings.TrimSuffix(query, h.config.MultiStatementDelimiter)
	query = strings.TrimSpace(query)
	if query == "" {
		return nil
	}

	ctx, cancel := contextWithTimeout(h.config.StatementTimeout)
	defer cancel()

	_, err := tx.ExecContext(ctx, query)
	if err != nil {
		return &database.Error{OrigErr: err, Err: "migration failed", Query: []byte(query)}
	}

	return nil
}

// ensureVersionTable checks if the migrations table exists and creates it if not.
func (h *Hana) ensureVersionTable() (err error) {
	var count int
	query := `SELECT COUNT(*) FROM SYS.TABLES WHERE SCHEMA_NAME = ? AND TABLE_NAME = ?`

	ctx, cancel := contextWithTimeout(h.config.StatementTimeout)
	defer cancel()

	err = h.db.QueryRowContext(ctx, query,
		h.config.SchemaName,
		h.config.MigrationsTable,
	).Scan(&count)
	if err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}

	if count == 1 {
		return nil
	} else if count != 0 {
		return ErrMigrationTableCount
	}

	query = fmt.Sprintf("CREATE ROW TABLE %s.%s (version BIGINT NOT NULL PRIMARY KEY, dirty BOOLEAN NOT NULL)", hdbDriver.Identifier(h.config.SchemaName), hdbDriver.Identifier(h.config.MigrationsTable))

	ctx, cancel = contextWithTimeout(h.config.StatementTimeout)
	defer cancel()

	_, err = h.db.ExecContext(ctx, query)
	if err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}

	return nil
}
