package hana

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	nurl "net/url"
	"strconv"
	"sync/atomic"
	"time"

	hdbDriver "github.com/SAP/go-hdb/driver"
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
)

func init() {
	database.Register("hdb", &Hana{})
}

var _ database.Driver = (*Hana)(nil)

var (
	DefaultMigrationsTable = "schema_migrations"

	ErrNilConfig               = fmt.Errorf("no config")
	ErrNoSchemaName            = fmt.Errorf("no schema name")
	ErrInvalidStatementTimeout = fmt.Errorf("invalid x-statement-timeout")
	ErrInvalidIsolationLevel   = fmt.Errorf("invalid x-isolation-level")
)

type Config struct {
	SchemaName       string
	MigrationsTable  string
	StatementTimeout time.Duration
	IsolationLevel   sql.IsolationLevel
}

type Hana struct {
	db       *sql.DB
	config   *Config
	isLocked atomic.Bool
}

func (h *Hana) Open(url string) (database.Driver, error) {
	purl, err := nurl.Parse(url)
	if err != nil {
		return nil, err
	}

	schemaName := purl.Query().Get("x-migrations-schema")
	if schemaName == "" {
		return nil, ErrNoSchemaName
	}

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
			return nil, fmt.Errorf("could not parse x-isolation-level: %w", err)
		}

		if isolationLevelInt < int(sql.LevelDefault) || isolationLevelInt > int(sql.LevelLinearizable) {
			return nil, fmt.Errorf("%w: %d", ErrInvalidIsolationLevel, isolationLevelInt)
		}

		isolationLevel = sql.IsolationLevel(isolationLevelInt)
	}

	dsn := migrate.FilterCustomQuery(purl).String()
	connector, err := hdbDriver.NewDSNConnector(dsn)
	if err != nil {
		return nil, err
	}

	connector.SetDefaultSchema(schemaName)
	db := sql.OpenDB(connector)

	return WithInstance(db, &Config{
		MigrationsTable:  migrationsTable,
		SchemaName:       schemaName,
		StatementTimeout: statementTimeout,
		IsolationLevel:   isolationLevel,
	})
}

func (h *Hana) Close() error {
	return h.db.Close()
}

func (h *Hana) Lock() error {
	if !h.isLocked.CompareAndSwap(false, true) {
		return database.ErrLocked
	}

	return nil
}

func (h *Hana) Unlock() error {
	if !h.isLocked.CompareAndSwap(true, false) {
		return database.ErrNotLocked
	}

	return nil
}

func (h *Hana) Run(migration io.Reader) error {
	migr, err := io.ReadAll(migration)
	if err != nil {
		return err
	}

	ctx := context.Background()
	if h.config.StatementTimeout != 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, h.config.StatementTimeout)
		defer cancel()
	}

	if _, err := h.db.ExecContext(ctx, string(migr)); err != nil {
		return database.Error{OrigErr: err, Err: "migration failed", Query: migr}
	}

	return nil
}

func (h *Hana) SetVersion(version int, dirty bool) error {
	tx, err := h.db.BeginTx(context.Background(), &sql.TxOptions{Isolation: h.config.IsolationLevel})
	if err != nil {
		return &database.Error{OrigErr: err, Err: "transaction start failed"}
	}

	query := `DELETE FROM "` + h.config.SchemaName + `"."` + h.config.MigrationsTable + `"`
	if _, err := tx.ExecContext(context.Background(), query); err != nil {
		if errRollback := tx.Rollback(); errRollback != nil {
			err = errors.Join(err, errRollback)
		}

		return &database.Error{OrigErr: err, Query: []byte(query)}
	}

	// Also re-write the schema version for nil dirty versions to prevent
	// empty schema version for failed down migration on the first migration.
	// See: https://github.com/golang-migrate/migrate/issues/330
	if version >= 0 || (version == database.NilVersion && dirty) {
		query = `INSERT INTO "` + h.config.SchemaName + `"."` + h.config.MigrationsTable + `" (version, dirty) VALUES (?, ?)`
		if _, err := tx.ExecContext(context.Background(), query, version, dirty); err != nil {
			if errRollback := tx.Rollback(); errRollback != nil {
				err = errors.Join(err, errRollback)
			}

			return &database.Error{OrigErr: err, Query: []byte(query)}
		}
	}

	if err := tx.Commit(); err != nil {
		return &database.Error{OrigErr: err, Err: "transaction commit failed"}
	}

	return nil
}

func (h *Hana) Version() (version int, dirty bool, err error) {
	query := `SELECT version, dirty FROM "` + h.config.SchemaName + `"."` + h.config.MigrationsTable + `" LIMIT 1`

	err = h.db.QueryRowContext(context.Background(), query).Scan(&version, &dirty)
	switch {
	case err == sql.ErrNoRows:
		return database.NilVersion, false, nil
	case err != nil:
		return 0, false, &database.Error{OrigErr: err, Query: []byte(query)}
	default:
		return version, dirty, nil
	}
}

func (h *Hana) Drop() (err error) {
	query := `SELECT TABLE_NAME FROM SYS.TABLES WHERE SCHEMA_NAME = ?`

	tables, err := h.db.QueryContext(context.Background(), query, h.config.SchemaName)
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
		if err := tables.Scan(&tableName); err != nil {
			return err
		}

		if tableName != "" {
			tableNames = append(tableNames, tableName)
		}
	}

	if err := tables.Err(); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}

	for _, t := range tableNames {
		query = `DROP TABLE "` + h.config.SchemaName + `"."` + t + `"`
		if _, err := h.db.ExecContext(context.Background(), query); err != nil {
			return &database.Error{OrigErr: err, Query: []byte(query)}
		}
	}

	return nil
}

func WithInstance(instance *sql.DB, config *Config) (database.Driver, error) {
	if config == nil {
		return nil, ErrNilConfig
	}

	if err := instance.PingContext(context.Background()); err != nil {
		return nil, err
	}

	hx := &Hana{
		db:     instance,
		config: config,
	}

	if config.MigrationsTable == "" {
		config.MigrationsTable = DefaultMigrationsTable
	}

	if err := hx.ensureVersionTable(); err != nil {
		return nil, err
	}

	return hx, nil
}

// ensureVersionTable checks if the migrations table exists and creates it if not.
func (h *Hana) ensureVersionTable() (err error) {
	if err = h.Lock(); err != nil {
		return err
	}

	defer func() {
		if e := h.Unlock(); e != nil {
			err = errors.Join(err, e)
		}
	}()

	var count int
	query := `SELECT COUNT(*) FROM SYS.TABLES WHERE SCHEMA_NAME = ? AND TABLE_NAME = ?`
	if err := h.db.QueryRowContext(context.Background(), query,
		h.config.SchemaName,
		h.config.MigrationsTable,
	).Scan(&count); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}

	if count == 1 {
		return nil
	}

	query = `CREATE ROW TABLE "` + h.config.SchemaName + `"."` + h.config.MigrationsTable + `" (version BIGINT NOT NULL PRIMARY KEY, dirty BOOLEAN NOT NULL)`
	if _, err = h.db.ExecContext(context.Background(), query); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}

	return nil
}
