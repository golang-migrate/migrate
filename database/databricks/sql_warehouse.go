//go:build go1.9
// +build go1.9

package databricks

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	_ "github.com/databricks/databricks-sql-go"
	"github.com/golang-migrate/migrate/v4/database"
	"github.com/golang-migrate/migrate/v4/database/multistmt"
	"go.uber.org/atomic"
)

const RegistrationKey = "databricks-sqlwarehouse"

func init() {
	db := SQLWarehouse{}
	database.Register(RegistrationKey, &db)
}

var (
	multiStmtDelimiter = []byte(";")

	DefaultMigrationsTable       = "schema_migrations"
	DefaultMultiStatementMaxSize = 10 * 1 << 20 // 10 MB
)

var (
	ErrNilConfig = fmt.Errorf("no config")
)

type Config struct {
	MigrationsTable       string
	MultiStatementEnabled bool
	DatabaseName          string
	StatementTimeout      time.Duration
	MultiStatementMaxSize int
}

type SQLWarehouse struct {
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

	conn := instance
	if conn == nil {
		return nil, fmt.Errorf("connection is nil")
	}

	px := &SQLWarehouse{
		db:     conn,
		config: config,
	}

	if err := px.ensureVersionTable(); err != nil {
		return nil, err
	}

	return px, nil
}

func (d *SQLWarehouse) Open(dsn string) (database.Driver, error) {
	dsn = strings.TrimPrefix(dsn, RegistrationKey+"://")

	db, err := sql.Open("databricks", dsn)
	if err != nil {
		// Developer notes: ensure the DSN Port is specified as 443 if the error message 'invalid DSN Port' is returned
		// Please see https://docs.databricks.com/en/dev-tools/go-sql-driver.html for correct formatting guidance
		return nil, err
	}

	px, err := WithInstance(db, &Config{
		DatabaseName:          dsn, // Adjust if necessary for proper parsing
		MigrationsTable:       DefaultMigrationsTable,
		StatementTimeout:      0, // Default timeout
		MultiStatementMaxSize: DefaultMultiStatementMaxSize,
	})

	if err != nil {
		return nil, err
	}

	return px, nil
}

func (d *SQLWarehouse) Close() error {
	return d.db.Close()
}

func (d *SQLWarehouse) Lock() error {
	return database.CasRestoreOnErr(&d.isLocked, false, true, database.ErrLocked, func() error {
		// Databricks SQL Warehouse does not support locking
		// Placeholder for actual lock code
		return nil
	})
}

func (d *SQLWarehouse) Unlock() error {
	return database.CasRestoreOnErr(&d.isLocked, true, false, database.ErrNotLocked, func() error {
		// Databricks SQL Warehouse does not support locking
		// Placeholder for actual lock code
		return nil
	})
}

func (d *SQLWarehouse) Run(migration io.Reader) error {
	if d.config.MultiStatementEnabled {
		var err error
		if e := multistmt.Parse(migration, multiStmtDelimiter, d.config.MultiStatementMaxSize, func(m []byte) bool {
			if err = d.runStatement(m); err != nil {
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
	return d.runStatement(migr)
}

func (d *SQLWarehouse) runStatement(statement []byte) error {
	ctx := context.Background()
	query := string(statement)
	if strings.TrimSpace(query) == "" {
		return nil
	}
	if _, err := d.db.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("migration failed: %w", err)
	}
	return nil
}

func (d *SQLWarehouse) SetVersion(version int, dirty bool) error {
	// Transactions are not supported yet, so we will manually clear and insert the version.

	// First, truncate the migrations table to remove any existing records.
	query := `TRUNCATE TABLE ` + d.config.MigrationsTable
	if _, err := d.db.ExecContext(context.Background(), query); err != nil {
		return fmt.Errorf("failed to truncate migration table: %w", err)
	}

	// Then, insert the new migration version and dirty flag.
	// Since Databricks SQL doesn't support positional parameters (`?`), we'll insert the values directly into the query.
	query = fmt.Sprintf(`INSERT INTO %s (version, dirty) VALUES (%d, %t)`, d.config.MigrationsTable, version, dirty)
	if _, err := d.db.ExecContext(context.Background(), query); err != nil {
		return fmt.Errorf("failed to insert version into migration table: %w", err)
	}

	return nil
}

func (d *SQLWarehouse) Version() (version int, dirty bool, err error) {
	query := `SELECT version, dirty FROM ` + d.config.MigrationsTable + ` LIMIT 1`
	err = d.db.QueryRowContext(context.Background(), query).Scan(&version, &dirty)
	if errors.Is(err, sql.ErrNoRows) {
		return -1, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("failed to get migration version: %w", err)
	}
	return version, dirty, nil
}

func (d *SQLWarehouse) Drop() (err error) {
	// Show and drop all tables in the current schema
	query := `SHOW TABLES`
	rows, err := d.db.QueryContext(context.Background(), query)
	if err != nil {
		return fmt.Errorf("failed to list tables: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return err
		}
		if tableName != "" {
			dropQuery := `DROP TABLE IF EXISTS ` + tableName
			if _, err := d.db.ExecContext(context.Background(), dropQuery); err != nil {
				return fmt.Errorf("failed to drop table %s: %w", tableName, err)
			}
		}
	}
	return nil
}

func (d *SQLWarehouse) ensureVersionTable() (err error) {
	// Check if the migrations table exists and create it if not
	query := `SHOW TABLES LIKE '` + d.config.MigrationsTable + `'`
	row := d.db.QueryRowContext(context.Background(), query)

	// SHOW TABLES returns (database, table_name, is_temporary)
	var dbName, tableName, isTemporary string
	if err := row.Scan(&dbName, &tableName, &isTemporary); errors.Is(err, sql.ErrNoRows) {
		// The migrations table doesn't exist, create it
		createTableQuery := `CREATE TABLE ` + d.config.MigrationsTable + ` (version BIGINT, dirty BOOLEAN)`
		if _, err := d.db.ExecContext(context.Background(), createTableQuery); err != nil {
			return fmt.Errorf("failed to create migrations table: %w", err)
		}
	} else if err != nil {
		return fmt.Errorf("failed to check migrations table: %w", err)
	}

	return nil
}
