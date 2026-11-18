//go:build go1.9

// Package dsql implements the database.Driver interface for Amazon Aurora DSQL.
package dsql

import (
	"context"
	"database/sql"
	"errors"
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
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib"
)

var (
	multiStmtDelimiter = []byte(";")

	DefaultMigrationsTable       = "schema_migrations"
	DefaultLockTable             = "schema_migrations_lock"
	DefaultMultiStatementMaxSize = 10 * 1 << 20 // 10 MB
)

var (
	ErrNilConfig      = errors.New("no config")
	ErrNoDatabaseName = errors.New("no database name")
	ErrNoSchema       = errors.New("no schema")
)

func init() {
	database.Register("dsql", &DSQL{})
}

type Config struct {
	MigrationsTable       string
	LockTable             string
	ForceLock             bool
	DatabaseName          string
	SchemaName            string
	StatementTimeout      time.Duration
	MultiStatementEnabled bool
	MultiStatementMaxSize int
}

type DSQL struct {
	conn     *sql.Conn
	db       *sql.DB
	isLocked atomic.Bool
	config   *Config
}

func WithInstance(instance *sql.DB, config *Config) (database.Driver, error) {
	if config == nil {
		return nil, ErrNilConfig
	}

	if err := instance.Ping(); err != nil {
		return nil, err
	}

	if config.DatabaseName == "" {
		query := `SELECT CURRENT_DATABASE()`
		if err := instance.QueryRow(query).Scan(&config.DatabaseName); err != nil {
			return nil, &database.Error{OrigErr: err, Query: []byte(query)}
		}
		if config.DatabaseName == "" {
			return nil, ErrNoDatabaseName
		}
	}

	if config.SchemaName == "" {
		query := `SELECT CURRENT_SCHEMA()`
		if err := instance.QueryRow(query).Scan(&config.SchemaName); err != nil {
			return nil, &database.Error{OrigErr: err, Query: []byte(query)}
		}
		if config.SchemaName == "" {
			return nil, ErrNoSchema
		}
	}

	if config.MigrationsTable == "" {
		config.MigrationsTable = DefaultMigrationsTable
	}
	if config.LockTable == "" {
		config.LockTable = DefaultLockTable
	}
	if config.MultiStatementMaxSize <= 0 {
		config.MultiStatementMaxSize = DefaultMultiStatementMaxSize
	}

	conn, err := instance.Conn(context.Background())
	if err != nil {
		return nil, err
	}

	d := &DSQL{conn: conn, db: instance, config: config}
	if err := d.ensureLockTable(); err != nil {
		_ = conn.Close()
		return nil, err
	}
	if err := d.ensureVersionTable(); err != nil {
		_ = conn.Close()
		return nil, err
	}

	return d, nil
}

func (d *DSQL) Open(rawURL string) (database.Driver, error) {
	purl, err := nurl.Parse(rawURL)
	if err != nil {
		return nil, err
	}

	// Aurora DSQL uses the PostgreSQL wire protocol. pgx expects a postgres URL.
	purl.Scheme = "postgres"
	db, err := sql.Open("pgx/v5", migrate.FilterCustomQuery(purl).String())
	if err != nil {
		return nil, err
	}

	statementTimeout := 0
	if value := purl.Query().Get("x-statement-timeout"); value != "" {
		statementTimeout, err = strconv.Atoi(value)
		if err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("unable to parse x-statement-timeout: %w", err)
		}
	}

	multiStatementMaxSize := DefaultMultiStatementMaxSize
	if value := purl.Query().Get("x-multi-statement-max-size"); value != "" {
		multiStatementMaxSize, err = strconv.Atoi(value)
		if err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("unable to parse x-multi-statement-max-size: %w", err)
		}
		if multiStatementMaxSize <= 0 {
			multiStatementMaxSize = DefaultMultiStatementMaxSize
		}
	}

	forceLock := false
	if value := purl.Query().Get("x-force-lock"); value != "" {
		forceLock, err = strconv.ParseBool(value)
		if err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("unable to parse x-force-lock: %w", err)
		}
	}

	multiStatementEnabled := false
	if value := purl.Query().Get("x-multi-statement"); value != "" {
		multiStatementEnabled, err = strconv.ParseBool(value)
		if err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("unable to parse x-multi-statement: %w", err)
		}
	}

	driver, err := WithInstance(db, &Config{
		DatabaseName:          strings.TrimPrefix(purl.Path, "/"),
		MigrationsTable:       purl.Query().Get("x-migrations-table"),
		LockTable:             purl.Query().Get("x-lock-table"),
		ForceLock:             forceLock,
		StatementTimeout:      time.Duration(statementTimeout) * time.Millisecond,
		MultiStatementEnabled: multiStatementEnabled,
		MultiStatementMaxSize: multiStatementMaxSize,
	})
	if err != nil {
		_ = db.Close()
		return nil, err
	}

	return driver, nil
}

func (d *DSQL) Close() error {
	connErr := d.conn.Close()
	dbErr := d.db.Close()
	return errors.Join(connErr, dbErr)
}

// Lock uses a row in a dedicated table because Aurora DSQL doesn't support
// PostgreSQL advisory locks. A process interrupted while holding the lock can
// leave a stale row; x-force-lock allows an operator to remove it deliberately.
func (d *DSQL) Lock() error {
	return database.CasRestoreOnErr(&d.isLocked, false, true, database.ErrLocked, func() error {
		lockID, err := database.GenerateAdvisoryLockId(
			d.config.DatabaseName,
			d.config.SchemaName,
			d.config.MigrationsTable,
		)
		if err != nil {
			return err
		}

		tx, err := d.conn.BeginTx(context.Background(), &sql.TxOptions{})
		if err != nil {
			return &database.Error{OrigErr: err, Err: "transaction start failed"}
		}
		defer func() { _ = tx.Rollback() }()

		lockTable := d.qualifiedTable(d.config.LockTable)
		if d.config.ForceLock {
			query := `DELETE FROM ` + lockTable + ` WHERE lock_id = $1`
			if _, err := tx.Exec(query, lockID); err != nil {
				return &database.Error{OrigErr: err, Err: "failed to clear migration lock", Query: []byte(query)}
			}
		}

		query := `INSERT INTO ` + lockTable + ` (lock_id) VALUES ($1)`
		if _, err := tx.Exec(query, lockID); err != nil {
			if isLockConflict(err) {
				return database.ErrLocked
			}
			return &database.Error{OrigErr: err, Err: "failed to set migration lock", Query: []byte(query)}
		}
		if err := tx.Commit(); err != nil {
			if isLockConflict(err) {
				return database.ErrLocked
			}
			return &database.Error{OrigErr: err, Err: "migration lock commit failed"}
		}
		return nil
	})
}

func (d *DSQL) Unlock() error {
	return database.CasRestoreOnErr(&d.isLocked, true, false, database.ErrNotLocked, func() error {
		lockID, err := database.GenerateAdvisoryLockId(
			d.config.DatabaseName,
			d.config.SchemaName,
			d.config.MigrationsTable,
		)
		if err != nil {
			return err
		}

		query := `DELETE FROM ` + d.qualifiedTable(d.config.LockTable) + ` WHERE lock_id = $1`
		if _, err := d.conn.ExecContext(context.Background(), query, lockID); err != nil {
			if isUndefinedTable(err) {
				return nil
			}
			return &database.Error{OrigErr: err, Err: "failed to release migration lock", Query: []byte(query)}
		}
		return nil
	})
}

// Run splits migrations into individual statements. Aurora DSQL allows only
// one DDL statement in a transaction and pgx otherwise sends a multi-statement
// Exec as one implicit transaction.
func (d *DSQL) Run(migration io.Reader) error {
	if !d.config.MultiStatementEnabled {
		statement, err := io.ReadAll(migration)
		if err != nil {
			return err
		}
		return d.runStatement(statement)
	}

	var runErr error
	parseErr := multistmt.Parse(migration, multiStmtDelimiter, d.config.MultiStatementMaxSize, func(statement []byte) bool {
		if runErr = d.runStatement(statement); runErr != nil {
			return false
		}
		return true
	})
	if parseErr != nil {
		return parseErr
	}
	return runErr
}

func (d *DSQL) runStatement(statement []byte) error {
	query := string(statement)
	if strings.TrimSpace(query) == "" {
		return nil
	}

	ctx := context.Background()
	if d.config.StatementTimeout != 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, d.config.StatementTimeout)
		defer cancel()
	}

	if _, err := d.conn.ExecContext(ctx, query); err != nil {
		return &database.Error{OrigErr: err, Err: "migration failed", Query: statement}
	}
	return nil
}

func (d *DSQL) SetVersion(version int, dirty bool) error {
	tx, err := d.conn.BeginTx(context.Background(), &sql.TxOptions{})
	if err != nil {
		return &database.Error{OrigErr: err, Err: "transaction start failed"}
	}

	query := `DELETE FROM ` + d.qualifiedTable(d.config.MigrationsTable)
	if _, err := tx.Exec(query); err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			err = errors.Join(err, rollbackErr)
		}
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}

	if version >= 0 || (version == database.NilVersion && dirty) {
		query = `INSERT INTO ` + d.qualifiedTable(d.config.MigrationsTable) + ` (version, dirty) VALUES ($1, $2)`
		if _, err := tx.Exec(query, version, dirty); err != nil {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				err = errors.Join(err, rollbackErr)
			}
			return &database.Error{OrigErr: err, Query: []byte(query)}
		}
	}

	if err := tx.Commit(); err != nil {
		return &database.Error{OrigErr: err, Err: "transaction commit failed"}
	}
	return nil
}

func (d *DSQL) Version() (version int, dirty bool, err error) {
	query := `SELECT version, dirty FROM ` + d.qualifiedTable(d.config.MigrationsTable) + ` LIMIT 1`
	err = d.conn.QueryRowContext(context.Background(), query).Scan(&version, &dirty)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return database.NilVersion, false, nil
	case isUndefinedTable(err):
		return database.NilVersion, false, nil
	case err != nil:
		return 0, false, &database.Error{OrigErr: err, Query: []byte(query)}
	default:
		return version, dirty, nil
	}
}

func (d *DSQL) Drop() (err error) {
	query := `SELECT table_name FROM information_schema.tables WHERE table_schema = $1 AND table_type = 'BASE TABLE'`
	rows, err := d.conn.QueryContext(context.Background(), query, d.config.SchemaName)
	if err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			err = errors.Join(err, closeErr)
		}
	}()

	var tables []string
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			return err
		}
		if table != d.config.LockTable {
			tables = append(tables, table)
		}
	}
	if err := rows.Err(); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}
	// Close before issuing DDL on the same pinned connection.
	if err := rows.Close(); err != nil {
		return err
	}

	// Drop the lock table last so Migrate can still call Unlock after Drop.
	lockTableExists, err := d.tableExists(d.config.LockTable)
	if err != nil {
		return err
	}
	if lockTableExists {
		tables = append(tables, d.config.LockTable)
	}
	for _, table := range tables {
		query = `DROP TABLE IF EXISTS ` + d.qualifiedTable(table) + ` CASCADE`
		if _, err := d.conn.ExecContext(context.Background(), query); err != nil {
			return &database.Error{OrigErr: err, Query: []byte(query)}
		}
	}
	return nil
}

func (d *DSQL) ensureVersionTable() (err error) {
	if err = d.Lock(); err != nil {
		return err
	}
	defer func() {
		if unlockErr := d.Unlock(); unlockErr != nil {
			err = errors.Join(err, unlockErr)
		}
	}()

	migrationsTableExists, err := d.tableExists(d.config.MigrationsTable)
	if err != nil {
		return err
	}
	if migrationsTableExists {
		return nil
	}
	query := `CREATE TABLE IF NOT EXISTS ` + d.qualifiedTable(d.config.MigrationsTable) +
		` (version BIGINT NOT NULL PRIMARY KEY, dirty BOOLEAN NOT NULL)`
	if _, err := d.conn.ExecContext(context.Background(), query); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}
	return nil
}

func (d *DSQL) ensureLockTable() error {
	lockTableExists, err := d.tableExists(d.config.LockTable)
	if err != nil {
		return err
	}
	if lockTableExists {
		return nil
	}
	query := `CREATE TABLE IF NOT EXISTS ` + d.qualifiedTable(d.config.LockTable) +
		` (lock_id TEXT NOT NULL PRIMARY KEY)`
	if _, err := d.conn.ExecContext(context.Background(), query); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}
	return nil
}

func (d *DSQL) tableExists(table string) (bool, error) {
	query := `SELECT EXISTS (` +
		`SELECT 1 FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2 AND table_type = 'BASE TABLE'` +
		`)`
	var exists bool
	if err := d.conn.QueryRowContext(context.Background(), query, d.config.SchemaName, table).Scan(&exists); err != nil {
		return false, &database.Error{OrigErr: err, Query: []byte(query)}
	}
	return exists, nil
}

func (d *DSQL) qualifiedTable(table string) string {
	return quoteIdentifier(d.config.SchemaName) + `.` + quoteIdentifier(table)
}

func isLockConflict(err error) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}
	return pgErr.SQLState() == pgerrcode.UniqueViolation || pgErr.SQLState() == pgerrcode.SerializationFailure
}

func isUndefinedTable(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.SQLState() == pgerrcode.UndefinedTable
}

func quoteIdentifier(name string) string {
	if end := strings.IndexRune(name, 0); end >= 0 {
		name = name[:end]
	}
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}
