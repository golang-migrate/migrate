//go:build go1.9
// +build go1.9

package postgresconn

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/getoutreach/migrate/v4"
	"io"
	"io/ioutil"
	nurl "net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"go.uber.org/atomic"

	"github.com/getoutreach/migrate/v4/database"
	"github.com/getoutreach/migrate/v4/database/multistmt"
	multierror "github.com/hashicorp/go-multierror"
	"github.com/lib/pq"
	"github.com/pkg/errors"
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
}

type Postgres struct {
	// Locking and unlocking need to use the same connection
	conn *sql.Conn
	//db       *sql.DB
	isLocked atomic.Bool
	// Open and WithConn need to guarantee that config is never nil
	config *Config
}

func WithConn(ctx context.Context, conn *sql.Conn, config *Config) (database.Driver, error) {
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
		var schemaName string
		if err := conn.QueryRowContext(ctx, query).Scan(&schemaName); err != nil {
			return nil, &database.Error{OrigErr: err, Query: []byte(query)}
		}

		if len(schemaName) == 0 {
			return nil, ErrNoSchema
		}

		config.SchemaName = schemaName
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
		conn: conn,
		//db:     instance,
		config: config,
	}

	if err := px.ensureVersionTable(); err != nil {
		return nil, errors.Wrap(err, "error ensuring version table")
	}

	return px, nil
}

// Open opens a database connection and returns wrapped driver
// this function exists to satisfy tests
func (p *Postgres) Open(url string) (database.Driver, error) {
	fmt.Printf("Open(%s)\n", url)
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
			return nil, fmt.Errorf("Unable to parse option x-migrations-table-quoted: %w", err)
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
			return nil, fmt.Errorf("Unable to parse option x-multi-statement: %w", err)
		}
	}

	conn, err := db.Conn(context.Background())
	if err != nil {
		return nil, err
	}

	config := Config{
		DatabaseName:          purl.Path,
		MigrationsTable:       migrationsTable,
		MigrationsTableQuoted: migrationsTableQuoted,
		StatementTimeout:      time.Duration(statementTimeout) * time.Millisecond,
		MultiStatementEnabled: multiStatementEnabled,
		MultiStatementMaxSize: multiStatementMaxSize,
	}
	fmt.Printf("config: %+v\n", config)
	px, err := WithConn(context.Background(), conn, &config)
	if err != nil {
		return nil, err
	}
	return px, nil
}

func (p *Postgres) Close() error {
	connErr := p.conn.Close()
	if connErr != nil {
		return fmt.Errorf("conn: %v", connErr)
	}
	return nil
}

// Lock https://www.postgresql.org/docs/9.6/static/explicit-locking.html#ADVISORY-LOCKS
func (p *Postgres) Lock() error {
	return database.CasRestoreOnErr(&p.isLocked, false, true, database.ErrLocked, func() error {
		aid, err := database.GenerateAdvisoryLockId(p.config.DatabaseName, p.config.migrationsSchemaName, p.config.migrationsTableName)
		if err != nil {
			return err
		}

		// This will wait indefinitely until the lock can be acquired.
		query := `SELECT pg_advisory_lock($1)`
		if _, err := p.conn.ExecContext(context.Background(), query, aid); err != nil {
			return &database.Error{OrigErr: err, Err: "try lock failed", Query: []byte(query)}
		}

		return nil
	})
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
		var currentSchema string
		if err := p.conn.QueryRowContext(context.Background(), "select current_schema()").Scan(&currentSchema); err != nil {
			return err
		}

		err := multistmt.Parse(migration, multiStmtDelimiter, p.config.MultiStatementMaxSize, p.config.SchemaName, func(m []byte) error {
			if err := p.runStatement(m); err != nil {
				return errors.Wrap(err, string(m))
			}
			return nil
		})
		return err
	}
	migr, err := ioutil.ReadAll(migration)
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

	// Also re-write the schema version for nil dirty versions to prevent
	// empty schema version for failed down migration on the first migration
	// See: https://github.com/getoutreach/migrate/issues/330
	if version >= 0 || (version == database.NilVersion && dirty) {
		query := fmt.Sprintf(`INSERT INTO %q.%q`+
			` (version, dirty, created_at) VALUES ($1, $2, now())`,
			p.config.migrationsSchemaName, p.config.migrationsTableName)
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
	query := fmt.Sprintf(`SELECT version, dirty FROM %q.%q`+
		` ORDER BY created_at desc LIMIT 1`,
		p.config.migrationsSchemaName, p.config.migrationsTableName)
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

	query = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %q.%q`+
		` (version bigint not null, dirty boolean not null)`,
		p.config.migrationsSchemaName, p.config.migrationsTableName)
	if _, err = p.conn.ExecContext(context.Background(), query); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}

	// add the created_at and info columns to track history and failures of migrations
	query = fmt.Sprintf(`ALTER TABLE %q.%q `+
		`ADD COLUMN IF NOT EXISTS created_at timestamp with time zone NULL, `+
		`ADD COLUMN IF NOT EXISTS info text NULL`,
		p.config.migrationsSchemaName, p.config.migrationsTableName)
	if _, err = p.conn.ExecContext(context.Background(), query); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}

	// adds index to the created_at to ensure queries ordering by created_at are snappy
	query = fmt.Sprintf(`CREATE INDEX IF NOT EXISTS idx_on_created_at on %q.%q (created_at)`,
		p.config.migrationsSchemaName, p.config.migrationsTableName)
	if _, err = p.conn.ExecContext(context.Background(), query); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}

	// ensure the new 'id' synthetic primary key exists
	// the old primary key on version only allowed one version to exist in the table
	// the new primary key allows many to rows with the same version to exist, but
	// only one row should be error free, if there are more than one row with the same
	// version we expect them to be from failures and the info column populated with
	// error details.
	if err := p.ensurePrimaryKeyExists(); err != nil {
		return &database.Error{OrigErr: err}
	}

	return nil
}

// ensurePrimaryKey will add new synthetic primary key and drop old primary key
func (p *Postgres) ensurePrimaryKeyExists() error {
	// adding primary key will fill in missing primary key column values with values from the sequence.
	// alter table schema_version add column id bigserial primary key
	ctx := context.Background()
	oldPrimaryKeyName := "schema_version_pkey"
	// Remove the old (column name: versio) primary key if it exists
	exists, err := p.primaryKeyExists(p.config.MigrationsTable, "version")
	if err != nil {
		return err
	}
	if exists {
		// drop the constraint but not the column, keep the version column
		_, err = p.conn.ExecContext(ctx,
			fmt.Sprintf(`ALTER TABLE %q DROP CONSTRAINT %s`,
				p.config.MigrationsTable, oldPrimaryKeyName))
		if err != nil {
			return errors.Wrapf(err, "error dropping %s", oldPrimaryKeyName)
		}
	}

	// Add the new primary key if it does not exist
	exists, err = p.primaryKeyExists(p.config.MigrationsTable, "id")
	if err != nil {
		return err
	}
	if !exists {
		// generated column will be schema_version_pkey
		_, err = p.conn.ExecContext(ctx,
			fmt.Sprintf(`ALTER TABLE %q ADD COLUMN id BIGSERIAL PRIMARY KEY`,
				p.config.MigrationsTable))
		if err != nil {
			return err
		}
	}
	return nil
}

// primaryKeyExists query for named table constraint
func (p *Postgres) primaryKeyExists(tableName, primaryKeyName string) (bool, error) {
	ctx := context.Background()
	stmt := fmt.Sprintf(`SELECT 1
FROM pg_index
  JOIN pg_attribute a ON a.attrelid = pg_index.indrelid AND a.attnum = ANY(pg_index.indkey)
  JOIN pg_class ON pg_index.indrelid = pg_class.oid
  JOIN pg_namespace on pg_namespace.oid = pg_class.relnamespace
WHERE pg_index.indrelid = %s::regclass
  AND  pg_index.indisprimary
  AND pg_namespace.nspname = current_schema()
  and a.attname = '%s'
AND format_type(a.atttypid, a.atttypmod) = 'bigint'`, tableName, primaryKeyName)
	fmt.Printf("stmt: %s\n", stmt)
	// We expect one row to come back, for the id bigserial(bigint) column
	rows := p.conn.QueryRowContext(ctx, stmt)
	var exists int
	err := rows.Scan(&exists)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}

	// some other kind of err
	if err != nil {
		return false, err
	}

	// rows exist for the primary key name
	return true, nil
}

// Failed set the current migration to failed and record the failure in the database
func (p *Postgres) Failed(version int, info string, err error) error {
	ctx := context.Background()
	stmt := fmt.Sprintf(`UPDATE %q.%q SET info = $1 where version = $2`,
		p.config.migrationsSchemaName, p.config.migrationsTableName)
	if _, err := p.conn.ExecContext(ctx, stmt, info, version); err != nil {
		return err
	}
	return nil
}