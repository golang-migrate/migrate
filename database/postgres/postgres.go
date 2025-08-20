//go:build go1.9

package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	nurl "net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"go.uber.org/atomic"

	"github.com/cenkalti/backoff/v4"
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
	"github.com/golang-migrate/migrate/v4/database/multistmt"
	"github.com/hashicorp/go-multierror"
	"github.com/lib/pq"
)

func init() {
	db := Postgres{}
	database.Register("postgres", &db)
	database.Register("postgresql", &db)
}

var (
	multiStmtDelimiter = []byte(";")

	DefaultMigrationsTable       = "schema_migrations"
	DefaultMultiStatementMaxSize = 10 * 1 << 20 // 10 MB

	DefaultLockInitialRetryInterval = 100 * time.Millisecond
	DefaultLockMaxRetryInterval     = 1000 * time.Millisecond
)

var (
	ErrNilConfig      = fmt.Errorf("no config")
	ErrNoDatabaseName = fmt.Errorf("no database name")
	ErrNoSchema       = fmt.Errorf("no schema")
	ErrDatabaseDirty  = fmt.Errorf("database is dirty")
)

type Config struct {
	MigrationsTable       string
	MigrationsTableQuoted bool
	MultiStatementEnabled bool
	DatabaseName          string
	SchemaName            string
	migrationsSchemaName  string
	migrationsTableName   string
	StatementTimeout      time.Duration
	MultiStatementMaxSize int
	Locking               LockConfig
}

type LockConfig struct {
	// InitialRetryInterval the initial (minimum) retry interval used for exponential backoff
	// to try acquire a lock
	InitialRetryInterval time.Duration

	// MaxRetryInterval the maximum retry interval. Once the exponential backoff reaches this limit,
	// the retry interval remains the same
	MaxRetryInterval time.Duration
}

type Postgres struct {
	// Locking and unlocking need to use the same connection
	conn     *sql.Conn
	db       *sql.DB
	isLocked atomic.Bool

	// Open and WithInstance need to guarantee that config is never nil
	config *Config
}

func WithConnection(ctx context.Context, conn *sql.Conn, config *Config) (*Postgres, error) {
	if config == nil {
		return nil, ErrNilConfig
	}

	if err := conn.PingContext(ctx); err != nil {
		return nil, err
	}

	if config.DatabaseName == "" {
		query := `SELECT CURRENT_DATABASE()`
		var databaseName string
		if err := conn.QueryRowContext(ctx, query).Scan(&databaseName); err != nil {
			return nil, &database.Error{OrigErr: err, Query: []byte(query)}
		}

		if len(databaseName) == 0 {
			return nil, ErrNoDatabaseName
		}

		config.DatabaseName = databaseName
	}

	if config.SchemaName == "" {
		query := `SELECT CURRENT_SCHEMA()`
		var schemaName sql.NullString
		if err := conn.QueryRowContext(ctx, query).Scan(&schemaName); err != nil {
			return nil, &database.Error{OrigErr: err, Query: []byte(query)}
		}

		if !schemaName.Valid {
			return nil, ErrNoSchema
		}

		config.SchemaName = schemaName.String
	}

	if len(config.MigrationsTable) == 0 {
		config.MigrationsTable = DefaultMigrationsTable
	}

	config.migrationsSchemaName = config.SchemaName
	config.migrationsTableName = config.MigrationsTable
	if config.MigrationsTableQuoted {
		re := regexp.MustCompile(`"(.*?)"`)
		result := re.FindAllStringSubmatch(config.MigrationsTable, -1)
		config.migrationsTableName = result[len(result)-1][1]
		if len(result) == 2 {
			config.migrationsSchemaName = result[0][1]
		} else if len(result) > 2 {
			return nil, fmt.Errorf("\"%s\" MigrationsTable contains too many dot characters", config.MigrationsTable)
		}
	}

	px := &Postgres{
		conn:   conn,
		config: config,
	}

	if err := px.ensureVersionTable(); err != nil {
		return nil, err
	}

	return px, nil
}

func WithInstance(instance *sql.DB, config *Config) (database.Driver, error) {
	ctx := context.Background()

	if err := instance.Ping(); err != nil {
		return nil, err
	}

	conn, err := instance.Conn(ctx)
	if err != nil {
		return nil, err
	}

	px, err := WithConnection(ctx, conn, config)
	if err != nil {
		return nil, err
	}
	px.db = instance
	return px, nil
}

func (p *Postgres) Open(url string) (database.Driver, error) {
	purl, err := nurl.Parse(url)
	if err != nil {
		return nil, err
	}

	db, err := sql.Open("postgres", migrate.FilterCustomQuery(purl).String())
	if err != nil {
		return nil, err
	}

	migrationsTable := purl.Query().Get("x-migrations-table")
	migrationsTableQuoted := false
	if s := purl.Query().Get("x-migrations-table-quoted"); len(s) > 0 {
		migrationsTableQuoted, err = strconv.ParseBool(s)
		if err != nil {
			return nil, fmt.Errorf("unable to parse option x-migrations-table-quoted: %w", err)
		}
	}
	if (len(migrationsTable) > 0) && (migrationsTableQuoted) && ((migrationsTable[0] != '"') || (migrationsTable[len(migrationsTable)-1] != '"')) {
		return nil, fmt.Errorf("x-migrations-table must be quoted (for instance '\"migrate\".\"schema_migrations\"') when x-migrations-table-quoted is enabled, current value is: %s", migrationsTable)
	}

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

	lockConfig := LockConfig{
		InitialRetryInterval: DefaultLockInitialRetryInterval,
		MaxRetryInterval:     DefaultLockMaxRetryInterval,
	}
	if s := purl.Query().Get("x-lock-retry-max-interval"); len(s) > 0 {
		maxRetryIntervalMillis, err := strconv.Atoi(s)
		if err != nil {
			return nil, fmt.Errorf("unable to parse option x-lock-retry-max-interval: %w", err)
		}
		maxRetryInterval := time.Duration(maxRetryIntervalMillis) * time.Millisecond
		if maxRetryInterval > DefaultLockInitialRetryInterval {
			lockConfig.MaxRetryInterval = maxRetryInterval
		}
	}

	px, err := WithInstance(db, &Config{
		DatabaseName:          purl.Path,
		MigrationsTable:       migrationsTable,
		MigrationsTableQuoted: migrationsTableQuoted,
		StatementTimeout:      time.Duration(statementTimeout) * time.Millisecond,
		MultiStatementEnabled: multiStatementEnabled,
		MultiStatementMaxSize: multiStatementMaxSize,
		Locking:               lockConfig,
	})

	if err != nil {
		return nil, err
	}

	return px, nil
}

func (p *Postgres) Close() error {
	connErr := p.conn.Close()
	var dbErr error
	if p.db != nil {
		dbErr = p.db.Close()
	}

	if connErr != nil || dbErr != nil {
		return fmt.Errorf("conn: %v, db: %v", connErr, dbErr)
	}
	return nil
}

// Lock tries to acquire an advisory lock and retries indefinitely with an exponential backoff strategy
// https://www.postgresql.org/docs/9.6/static/explicit-locking.html#ADVISORY-LOCKS
func (p *Postgres) Lock() error {
	return database.CasRestoreOnErr(&p.isLocked, false, true, database.ErrLocked, func() error {
		backOff := p.config.Locking.nonStopBackoff()
		err := backoff.Retry(func() error {
			ok, err := p.tryLock()
			if err != nil {
				return fmt.Errorf("p.tryLock: %w", err)
			}

			if ok {
				return nil
			}

			return fmt.Errorf("could not acquire lock") // causes retry
		}, backOff)

		return err
	})
}

func (p *Postgres) tryLock() (bool, error) {
	aid, err := database.GenerateAdvisoryLockId(p.config.DatabaseName, p.config.migrationsSchemaName, p.config.migrationsTableName)
	if err != nil {
		return false, err
	}

	// https://www.postgresql.org/docs/current/functions-admin.html#FUNCTIONS-ADVISORY-LOCKS
	// should always return true or false
	query := `SELECT pg_try_advisory_lock($1)`
	var ok bool
	if err := p.conn.QueryRowContext(context.Background(), query, aid).Scan(&ok); err != nil {
		return false, &database.Error{OrigErr: err, Err: "pg_try_advisory_lock failed", Query: []byte(query)}
	}

	return ok, nil
}

func (p *Postgres) Unlock() error {
	return database.CasRestoreOnErr(&p.isLocked, true, false, database.ErrNotLocked, func() error {
		aid, err := database.GenerateAdvisoryLockId(p.config.DatabaseName, p.config.migrationsSchemaName, p.config.migrationsTableName)
		if err != nil {
			return err
		}

		query := `SELECT pg_advisory_unlock($1)`
		if _, err := p.conn.ExecContext(context.Background(), query, aid); err != nil {
			return &database.Error{OrigErr: err, Query: []byte(query)}
		}
		return nil
	})
}

func (p *Postgres) Run(migration io.Reader) error {
	if p.config.MultiStatementEnabled {
		var err error
		if e := multistmt.Parse(migration, multiStmtDelimiter, p.config.MultiStatementMaxSize, func(m []byte) bool {
			if err = p.runStatement(m); err != nil {
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
	return p.runStatement(migr)
}

func (p *Postgres) runStatement(statement []byte) error {
	ctx := context.Background()
	if p.config.StatementTimeout != 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, p.config.StatementTimeout)
		defer cancel()
	}
	query := string(statement)
	if strings.TrimSpace(query) == "" {
		return nil
	}
	if _, err := p.conn.ExecContext(ctx, query); err != nil {
		if pgErr, ok := err.(*pq.Error); ok {
			var line uint
			var col uint
			var lineColOK bool
			if pgErr.Position != "" {
				if pos, err := strconv.ParseUint(pgErr.Position, 10, 64); err == nil {
					line, col, lineColOK = computeLineFromPos(query, int(pos))
				}
			}
			message := fmt.Sprintf("migration failed: %s", pgErr.Message)
			if lineColOK {
				message = fmt.Sprintf("%s (column %d)", message, col)
			}
			if pgErr.Detail != "" {
				message = fmt.Sprintf("%s, %s", message, pgErr.Detail)
			}
			return database.Error{OrigErr: err, Err: message, Query: statement, Line: line}
		}
		return database.Error{OrigErr: err, Err: "migration failed", Query: statement}
	}
	return nil
}

func computeLineFromPos(s string, pos int) (line uint, col uint, ok bool) {
	// replace crlf with lf
	s = strings.Replace(s, "\r\n", "\n", -1)
	// pg docs: pos uses index 1 for the first character, and positions are measured in characters not bytes
	runes := []rune(s)
	if pos > len(runes) {
		return 0, 0, false
	}
	sel := runes[:pos]
	line = uint(runesCount(sel, newLine) + 1)
	col = uint(pos - 1 - runesLastIndex(sel, newLine))
	return line, col, true
}

const newLine = '\n'

func runesCount(input []rune, target rune) int {
	var count int
	for _, r := range input {
		if r == target {
			count++
		}
	}
	return count
}

func runesLastIndex(input []rune, target rune) int {
	for i := len(input) - 1; i >= 0; i-- {
		if input[i] == target {
			return i
		}
	}
	return -1
}

func (p *Postgres) SetVersion(version int, dirty bool) error {
	tx, err := p.conn.BeginTx(context.Background(), &sql.TxOptions{})
	if err != nil {
		return &database.Error{OrigErr: err, Err: "transaction start failed"}
	}

	query := `TRUNCATE ` + pq.QuoteIdentifier(p.config.migrationsSchemaName) + `.` + pq.QuoteIdentifier(p.config.migrationsTableName)
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
		query = `INSERT INTO ` + pq.QuoteIdentifier(p.config.migrationsSchemaName) + `.` + pq.QuoteIdentifier(p.config.migrationsTableName) + ` (version, dirty) VALUES ($1, $2)`
		if _, err := tx.Exec(query, version, dirty); err != nil {
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

func (p *Postgres) Version() (version int, dirty bool, err error) {
	query := `SELECT version, dirty FROM ` + pq.QuoteIdentifier(p.config.migrationsSchemaName) + `.` + pq.QuoteIdentifier(p.config.migrationsTableName) + ` LIMIT 1`
	err = p.conn.QueryRowContext(context.Background(), query).Scan(&version, &dirty)
	switch {
	case err == sql.ErrNoRows:
		return database.NilVersion, false, nil

	case err != nil:
		if e, ok := err.(*pq.Error); ok {
			if e.Code.Name() == "undefined_table" {
				return database.NilVersion, false, nil
			}
		}
		return 0, false, &database.Error{OrigErr: err, Query: []byte(query)}

	default:
		return version, dirty, nil
	}
}

func (p *Postgres) Drop() (err error) {
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
			query = `DROP TABLE IF EXISTS ` + pq.QuoteIdentifier(t) + ` CASCADE`
			if _, err := p.conn.ExecContext(context.Background(), query); err != nil {
				return &database.Error{OrigErr: err, Query: []byte(query)}
			}
		}
	}

	return nil
}

// ensureVersionTable checks if versions table exists and, if not, creates it.
// Note that this function locks the database, which deviates from the usual
// convention of "caller locks" in the Postgres type.
func (p *Postgres) ensureVersionTable() (err error) {
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

	// This block checks whether the `MigrationsTable` already exists. This is useful because it allows read only postgres
	// users to also check the current version of the schema. Previously, even if `MigrationsTable` existed, the
	// `CREATE TABLE IF NOT EXISTS...` query would fail because the user does not have the CREATE permission.
	// Taken from https://github.com/mattes/migrate/blob/master/database/postgres/postgres.go#L258
	query := `SELECT COUNT(1) FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2 LIMIT 1`
	row := p.conn.QueryRowContext(context.Background(), query, p.config.migrationsSchemaName, p.config.migrationsTableName)

	var count int
	err = row.Scan(&count)
	if err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}

	if count == 1 {
		return nil
	}

	query = `CREATE TABLE IF NOT EXISTS ` + pq.QuoteIdentifier(p.config.migrationsSchemaName) + `.` + pq.QuoteIdentifier(p.config.migrationsTableName) + ` (version bigint not null primary key, dirty boolean not null)`
	if _, err = p.conn.ExecContext(context.Background(), query); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}

	return nil
}

func (l *LockConfig) nonStopBackoff() backoff.BackOff {
	b := backoff.NewExponentialBackOff()
	b.InitialInterval = l.InitialRetryInterval
	b.MaxInterval = l.MaxRetryInterval
	b.MaxElapsedTime = 0 // this backoff won't stop
	b.Reset()

	return b
}
