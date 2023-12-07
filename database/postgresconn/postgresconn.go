//go:build go1.9
// +build go1.9

package postgresconn

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	nurl "net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/getoutreach/migrate/v4"

	"go.uber.org/atomic"

	"github.com/hashicorp/go-multierror"
	"github.com/lib/pq"
	"github.com/pkg/errors"

	"github.com/getoutreach/migrate/v4/database"
)

func init() {
	db := Postgres{}
	database.Register("postgres", &db)
	database.Register("postgresql", &db)
}

var (
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
	conn     *sql.Conn
	isLocked atomic.Bool
	// Open and WithConn need to guarantee that config is never nil
	config *Config
	// tx transaction surrounding migration
	tx *sql.Tx
	// context used during migrations
	ctx context.Context
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
		conn:   conn,
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
	purl, err := nurl.Parse(url)
	if err != nil {
		return nil, err
	}

	db, err := sql.Open("postgres", migrate.FilterCustomQuery(purl).String())
	if err != nil {
		return nil, err
	}

	conn, err := db.Conn(context.Background())
	if err != nil {
		return nil, err
	}

	config := Config{
		DatabaseName:          purl.Path,
		MigrationsTable:       DefaultMigrationsTable,
		MultiStatementMaxSize: DefaultMultiStatementMaxSize,
	}
	px, err := WithConn(context.Background(), conn, &config)
	if err != nil {
		return nil, err
	}
	return px, nil
}

func (p *Postgres) Close() error {
	err := p.conn.Close()
	if err != nil {
		return fmt.Errorf("conn: %w", err)
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
	buf, err := io.ReadAll(migration)
	if err != nil {
		return errors.Wrap(err, "error reading migration")
	}

	ctx := p.context()
	if p.config.StatementTimeout != 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(p.ctx, p.config.StatementTimeout)
		defer cancel()
	}

	if p.config.SchemaName != "" {
		buf = bytes.ReplaceAll(buf, []byte("<SCHEMA_NAME>"),
			[]byte(p.config.SchemaName))
	}

	if _, err := p.conn.ExecContext(ctx, string(buf)); err != nil {
		if pgErr, ok := err.(*pq.Error); ok {
			var line uint
			var col uint
			var lineColOK bool
			if pgErr.Position != "" {
				if pos, err := strconv.ParseUint(pgErr.Position, 10, 64); err == nil {
					line, col, lineColOK = computeLineFromPos(string(buf), int(pos))
				}
			}
			message := fmt.Sprintf("migration failed: %s", pgErr.Message)
			if lineColOK {
				message = fmt.Sprintf("%s (column %d)", message, col)
			}
			if pgErr.Detail != "" {
				message = fmt.Sprintf("%s, %s", message, pgErr.Detail)
			}
			return database.Error{OrigErr: err, Err: message, Query: buf, Line: line}
		}

		return database.Error{OrigErr: err, Err: "migration failed", Query: buf}
	}

	// exec was successful, commit here, then nothing to rollback
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
	// This function used to use its own transaction for writing
	// dirty to the schema_version. But since we moved to externally
	// managed transaction, we want the version to rollback when the
	// migration fails. At some point it probably makes sense to remove
	// dirty flag.

	// check for in progress version, if it exists use the in-progress
	// version to record the dirty, info etc. values.
	row := p.conn.QueryRowContext(p.context(),
		fmt.Sprintf(
			`SELECT id FROM %q.%q WHERE version = $1 ORDER BY created_at DESC limit 1`,
			p.config.migrationsSchemaName, p.config.migrationsTableName), version)
	var id int64
	if err := row.Scan(&id); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// Also re-write the schema version for nil dirty versions to prevent
			// empty schema version for failed down migration on the first migration
			// See: https://github.com/getoutreach/migrate/issues/330
			stmt := fmt.Sprintf(`INSERT INTO %q.%q`+
				` (version, dirty, created_at) VALUES ($1, $2, now())`,
				p.config.migrationsSchemaName, p.config.migrationsTableName)
			if _, err := p.conn.ExecContext(p.context(), stmt, version, dirty); err != nil {
				return &database.Error{OrigErr: err, Query: []byte(stmt)}
			}
		}
	} else {
		stmt := fmt.Sprintf(
			`UPDATE %q.%q SET dirty = $1, updated_at = now() WHERE id = $2`,
			p.config.migrationsSchemaName,
			p.config.migrationsTableName)
		if _, err := p.conn.ExecContext(p.context(), stmt, dirty, id); err != nil {
			return &database.Error{OrigErr: err, Query: []byte(stmt)}
		}
	}

	return nil
}

// Version get version from schema version table
func (p *Postgres) Version() (*database.Version, error) {
	stmt := fmt.Sprintf(`SELECT version, dirty, info, current_schema() FROM %q.%q`+
		` ORDER BY created_at desc nulls last LIMIT 1`,
		p.config.migrationsSchemaName, p.config.migrationsTableName)

	var (
		version       int
		dirty         bool
		info          string
		infoStr       sql.NullString
		currentSchema string
	)
	err := p.conn.QueryRowContext(p.context(), stmt).Scan(&version, &dirty,
		&infoStr, &currentSchema)
	if infoStr.Valid {
		info = infoStr.String
	}
	switch {
	case err == sql.ErrNoRows:
		return &database.Version{
			Version: database.NilVersion,
			Dirty:   false,
			Info:    info,
			Schema:  currentSchema}, nil
	case err != nil:
		if e, ok := err.(*pq.Error); ok {
			if e.Code.Name() == "undefined_table" {
				return &database.Version{
						Version: database.NilVersion,
						Dirty:   false,
						Info:    info,
						Schema:  currentSchema},
					&database.Error{OrigErr: e, Query: []byte(stmt)}
			}
		}
		return &database.Version{
				Version: 0,
				Dirty:   false,
				Info:    info,
				Schema:  currentSchema},
			&database.Error{OrigErr: err, Query: []byte(stmt)}
	default:
		return &database.Version{
			Version: version,
			Dirty:   dirty,
			Info:    info,
			Schema:  currentSchema}, nil
	}
}

func (p *Postgres) Drop() (err error) {
	// select all tables in current schema
	stmt := `SELECT table_name FROM information_schema.tables WHERE table_schema=(SELECT current_schema()) AND table_type='BASE TABLE'`
	tables, err := p.conn.QueryContext(p.context(), stmt)
	if err != nil {
		return &database.Error{OrigErr: err, Query: []byte(stmt)}
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
		return &database.Error{OrigErr: err, Query: []byte(stmt)}
	}

	if len(tableNames) > 0 {
		// delete one by one ...
		for _, t := range tableNames {
			stmt = `DROP TABLE IF EXISTS ` + pq.QuoteIdentifier(t) + ` CASCADE`
			if _, err := p.conn.ExecContext(p.context(), stmt); err != nil {
				return &database.Error{OrigErr: err, Query: []byte(stmt)}
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

	stmt := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %q.%q`+
		` (version bigint not null, dirty boolean not null)`,
		p.config.migrationsSchemaName, p.config.migrationsTableName)
	if _, err = p.conn.ExecContext(p.context(), stmt); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(stmt)}
	}

	// add the created_at and info columns to track history and failures of migrations
	stmt = fmt.Sprintf(`ALTER TABLE %q.%q `+
		`ADD COLUMN IF NOT EXISTS created_at timestamp with time zone NULL, `+
		`ADD COLUMN IF NOT EXISTS updated_at timestamp with time zone NULL, `+
		`ADD COLUMN IF NOT EXISTS info text NULL`,
		p.config.migrationsSchemaName, p.config.migrationsTableName)
	if _, err = p.conn.ExecContext(p.context(), stmt); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(stmt)}
	}

	// adds index to the created_at to ensure queries ordering by created_at are snappy
	stmt = fmt.Sprintf(`CREATE INDEX IF NOT EXISTS idx_on_created_at on %q.%q (created_at)`,
		p.config.migrationsSchemaName, p.config.migrationsTableName)
	if _, err = p.conn.ExecContext(p.context(), stmt); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(stmt)}
	}

	// ensure the new 'id' synthetic primary key exists
	// the old primary key on version only allowed one version to exist in the table
	// the new primary key allows many rows with the same version to exist, but
	// only one row should be error free, if there are more than one row with the same
	// version we expect them to be from failures and the info column populated with
	// error details.
	if err := p.ensurePrimaryKeyExists(); err != nil {
		return &database.Error{OrigErr: err}
	}

	if err := p.ensureUniqueConstraintExists(); err != nil {
		return &database.Error{OrigErr: err}
	}

	return nil
}

// ensurePrimaryKey will add new synthetic primary key and drop old primary key
func (p *Postgres) ensurePrimaryKeyExists() error {
	// adding primary key will fill in missing primary key column values with values from the sequence.
	// alter table schema_version add column id bigserial primary key
	oldPrimaryKeyName := "schema_version_pkey"
	// Remove the old (column name: version) primary key if it exists
	exists, err := p.primaryKeyExists(p.config.MigrationsTable, "version")
	if err != nil {
		return err
	}
	if exists {
		// drop the constraint but not the column, keep the version column
		_, err = p.conn.ExecContext(p.context(),
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
		_, err = p.conn.ExecContext(p.context(),
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
	stmt := fmt.Sprintf(`SELECT 1
FROM pg_index
  JOIN pg_attribute a ON a.attrelid = pg_index.indrelid AND a.attnum = ANY(pg_index.indkey)
  JOIN pg_class ON pg_index.indrelid = pg_class.oid
  JOIN pg_namespace on pg_namespace.oid = pg_class.relnamespace
WHERE pg_index.indrelid = '%s'::regclass
  AND  pg_index.indisprimary
  AND pg_namespace.nspname = current_schema()
  and a.attname = '%s'
AND format_type(a.atttypid, a.atttypmod) = 'bigint'`, tableName, primaryKeyName)
	// We expect one row to come back, for the id bigserial(bigint) column
	rows := p.conn.QueryRowContext(p.context(), stmt)
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

// ensureUniqueConstraintExists will add new unique constraint to the schema version table
func (p *Postgres) ensureUniqueConstraintExists() error {
	exists, err := p.uniqueConstraintExists(p.config.MigrationsTable, "version")
	if err != nil {
		return err
	}
	if exists {
		return nil
	}

	_, err = p.conn.ExecContext(p.context(),
		fmt.Sprintf(`ALTER TABLE %q ADD CONSTRAINT unique_version UNIQUE (version)`,
			p.config.MigrationsTable))
	if err != nil {
		return err
	}
	return nil
}

// uniqueConstraintExists check existence of unique constraint
// only supports single column unique check, could add array support for multiple columns
func (p *Postgres) uniqueConstraintExists(tableName, columnName string) (bool, error) {
	stmt := fmt.Sprintf(`SELECT 1
FROM pg_index
  JOIN pg_attribute a ON a.attrelid = pg_index.indrelid AND a.attnum = ANY(pg_index.indkey)
  JOIN pg_class ON pg_index.indrelid = pg_class.oid
  JOIN pg_namespace on pg_namespace.oid = pg_class.relnamespace
WHERE pg_index.indrelid = '%s'::regclass
  AND  pg_index.indisunique
  AND pg_namespace.nspname = current_schema()
  and a.attname = '%s'
AND format_type(a.atttypid, a.atttypmod) = 'bigint'`, tableName, columnName)
	// We expect one row to come back, for the version column and
	rows := p.conn.QueryRowContext(p.context(), stmt)
	var exists int
	err := rows.Scan(&exists)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}

	// some other kind of err
	if err != nil {
		return false, err
	}

	// rows exist
	return true, nil
}

// SetFailed set the current migration to failed and record the failure in the database
func (p *Postgres) SetFailed(version int, err error) error {
	stmt := fmt.Sprintf(`UPDATE %q.%q SET info = $1 where version = $2`,
		p.config.migrationsSchemaName, p.config.migrationsTableName)
	if _, err := p.conn.ExecContext(p.context(), stmt, fmt.Sprintf("%+v", err), version); err != nil {
		return err
	}
	return nil
}

// Begin begins transaction
func (p *Postgres) Begin() error {
	if p.tx != nil {
		return fmt.Errorf("transaction already started")
	}

	p.ctx = context.Background()
	tx, err := p.conn.BeginTx(p.ctx, nil)
	if err != nil {
		return err
	}
	// capture tx so we know a transaction was started
	p.tx = tx
	return nil
}

// Commit commits in progress transaction
func (p *Postgres) Commit() error {
	if p.tx == nil {
		return fmt.Errorf("no transaction in progress")
	}

	defer func() {
		// reset p.tx so a new transaction can be started
		p.tx = nil
	}()

	if err := p.tx.Commit(); err != nil {
		return err
	}
	return nil
}

// Rollback rolls back in progress transaction
func (p *Postgres) Rollback() error {
	if p.tx == nil {
		return fmt.Errorf("no transaction in progress")
	}

	defer func() {
		// reset p.tx so a new transaction can be started
		p.tx = nil
	}()

	if err := p.tx.Rollback(); err != nil {
		return err
	}

	return nil
}

// Context returns the a context that was created when transactions begins, or a
// background context if no context is set(no transaction started).
func (p *Postgres) context() context.Context {
	if p.ctx == nil {
		p.ctx = context.Background()
	}
	return p.ctx
}
