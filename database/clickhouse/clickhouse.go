package clickhouse

import (
	"database/sql"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"strings"
	"time"

	"go.uber.org/atomic"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
	"github.com/golang-migrate/migrate/v4/database/multistmt"
	"github.com/hashicorp/go-multierror"
)

var (
	multiStmtDelimiter = []byte(";")

	DefaultMigrationsTable       = "schema_migrations"
	DefaultMigrationsTableEngine = "TinyLog"
	DefaultMultiStatementMaxSize = 10 * 1 << 20 // 10 MB

	ErrNilConfig = fmt.Errorf("no config")
)

type Config struct {
	DatabaseName          string
	ClusterName           string
	MigrationsTable       string
	MigrationsTableEngine string
	MultiStatementEnabled bool
	MultiStatementMaxSize int
}

func init() {
	database.Register("clickhouse", &ClickHouse{})
}

func WithInstance(conn *sql.DB, config *Config) (database.Driver, error) {
	if config == nil {
		return nil, ErrNilConfig
	}

	if err := conn.Ping(); err != nil {
		return nil, err
	}

	ch := &ClickHouse{
		conn:   conn,
		config: config,
	}

	if err := ch.init(); err != nil {
		return nil, err
	}

	return ch, nil
}

type ClickHouse struct {
	conn     *sql.DB
	config   *Config
	isLocked atomic.Bool
}

func (ch *ClickHouse) Open(dsn string) (database.Driver, error) {
	purl, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}

	// "Our" setting (not a ClickHouse setting), resolved from query params.
	multiStatementMaxSize := DefaultMultiStatementMaxSize
	if s := purl.Query().Get("x-multi-statement-max-size"); len(s) > 0 {
		multiStatementMaxSize, err = strconv.Atoi(s)
		if err != nil {
			return nil, err
		}
	}

	// Another extended setting for our config purposes.
	migrationsTableEngine := DefaultMigrationsTableEngine
	if s := purl.Query().Get("x-migrations-table-engine"); len(s) > 0 {
		migrationsTableEngine = s
	}

	// If the DSN specifies the DB using both the path
	// and the query parameter, they must match.
	// Otherwise, we honor whatever is explicitly set.
	var targetDatabase string
	urlParamDB := purl.Query().Get("database")
	urlPathDB := strings.TrimPrefix(purl.Path, "/")
	if len(urlPathDB) != 0 && len(urlParamDB) != 0 && urlPathDB != urlParamDB {
		return nil, fmt.Errorf("DSN path-specified DB '%s' does not match and query parameter DB '%s'",
			urlPathDB, urlParamDB)
	} else if len(urlPathDB) != 0 {
		targetDatabase = urlPathDB
	} else {
		targetDatabase = urlParamDB
	}

	// Remove extended options from what we'll send to ClickHouse.
	// ClickHouse will reject unsupported settings rather
	// than silently fail. We also set the target DB in both the
	// path and the query parameter to ensure that both the legacy
	// v1 driver and the v2 driver target the correct database
	// and never split the schema migrations table and migrations
	// themselves across two databases due to misconfiguration.
	q := migrate.FilterCustomQuery(purl)
	q.Scheme = "tcp"
	q.Path = targetDatabase
	queryWithDatabase := q.Query()
	queryWithDatabase.Set("database", targetDatabase)
	q.RawQuery = queryWithDatabase.Encode()
	conn, err := sql.Open("clickhouse", q.String())
	if err != nil {
		return nil, err
	}

	ch = &ClickHouse{
		conn: conn,
		config: &Config{
			MigrationsTable:       purl.Query().Get("x-migrations-table"),
			MigrationsTableEngine: migrationsTableEngine,
			DatabaseName:          targetDatabase,
			ClusterName:           purl.Query().Get("x-cluster-name"),
			MultiStatementEnabled: purl.Query().Get("x-multi-statement") == "true",
			MultiStatementMaxSize: multiStatementMaxSize,
		},
	}

	if err := ch.init(); err != nil {
		return nil, err
	}

	return ch, nil
}

func (ch *ClickHouse) init() error {
	var connectionDatabase string
	if err := ch.conn.QueryRow("SELECT currentDatabase()").Scan(&connectionDatabase); err != nil {
		return err
	}
	if len(ch.config.DatabaseName) == 0 {
		ch.config.DatabaseName = connectionDatabase
	} else if connectionDatabase != ch.config.DatabaseName {
		// If a library user initializes us with WithInstance instead of Open,
		// it is possible for the connection to be created targeting any database,
		// including the default database. In such a case, migration-running code
		// below would apply migrations to the connection's associated database
		// even if the operations related to the schema migration table were
		// correctly scoped to the configured database. Fail hard here to prevent
		// this from happening.
		return fmt.Errorf(
			"provided connection using DB '%s' but config targets '%s'",
			connectionDatabase,
			ch.config.DatabaseName,
		)
	}

	if len(ch.config.MigrationsTable) == 0 {
		ch.config.MigrationsTable = DefaultMigrationsTable
	}

	if ch.config.MultiStatementMaxSize <= 0 {
		ch.config.MultiStatementMaxSize = DefaultMultiStatementMaxSize
	}

	if len(ch.config.MigrationsTableEngine) == 0 {
		ch.config.MigrationsTableEngine = DefaultMigrationsTableEngine
	}

	return ch.ensureVersionTable()
}

func (ch *ClickHouse) Run(r io.Reader) error {
	if ch.config.MultiStatementEnabled {
		var err error
		if e := multistmt.Parse(r, multiStmtDelimiter, ch.config.MultiStatementMaxSize, func(m []byte) bool {
			tq := strings.TrimSpace(string(m))
			if tq == "" {
				return true
			}
			if _, e := ch.conn.Exec(string(m)); e != nil {
				err = database.Error{OrigErr: e, Err: "migration failed", Query: m}
				return false
			}
			return true
		}); e != nil {
			return e
		}
		return err
	}

	migration, err := io.ReadAll(r)
	if err != nil {
		return err
	}

	if _, err := ch.conn.Exec(string(migration)); err != nil {
		return database.Error{OrigErr: err, Err: "migration failed", Query: migration}
	}

	return nil
}
func (ch *ClickHouse) Version() (int, bool, error) {
	var (
		version int
		dirty   uint8
		query   = fmt.Sprintf("SELECT version, dirty FROM %s ORDER BY sequence DESC LIMIT 1", ch.migrationTableReference())
	)
	if err := ch.conn.QueryRow(query).Scan(&version, &dirty); err != nil {
		if err == sql.ErrNoRows {
			return database.NilVersion, false, nil
		}
		return 0, false, &database.Error{OrigErr: err, Query: []byte(query)}
	}
	return version, dirty == 1, nil
}

func (ch *ClickHouse) SetVersion(version int, dirty bool) error {
	var (
		bool = func(v bool) uint8 {
			if v {
				return 1
			}
			return 0
		}
		tx, err = ch.conn.Begin()
	)
	if err != nil {
		return err
	}

	query := fmt.Sprintf(
		"INSERT INTO %s (version, dirty, sequence) VALUES (?, ?, ?)",
		ch.migrationTableReference(),
	)
	stmt, err := tx.Prepare(query)
	if err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			return fmt.Errorf("error during prepare statement %w and rollback %s", err, rollbackErr)
		}

		return err
	}

	if _, err := stmt.Exec(int64(version), bool(dirty), uint64(time.Now().UnixNano())); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}

	return tx.Commit()
}

// ensureVersionTable checks if versions table exists and, if not, creates it.
// Note that this function locks the database, which deviates from the usual
// convention of "caller locks" in the ClickHouse type.
func (ch *ClickHouse) ensureVersionTable() (err error) {
	if err = ch.Lock(); err != nil {
		return err
	}

	defer func() {
		if e := ch.Unlock(); e != nil {
			if err == nil {
				err = e
			} else {
				err = multierror.Append(err, e)
			}
		}
	}()

	var (
		table               string
		tableExistenceQuery = fmt.Sprintf(
			"SHOW TABLES FROM %s LIKE '%s'",
			ch.dbReference(),
			ch.config.MigrationsTable, // Escaping this is syntactically incorrect (LIKE expects a quoted string)
		)
	)

	// check if migration table exists
	if err = ch.conn.QueryRow(tableExistenceQuery).Scan(&table); err != nil {
		if err != sql.ErrNoRows {
			return &database.Error{OrigErr: err, Query: []byte(tableExistenceQuery)}
		}
	} else {
		return nil
	}

	// if not, create the empty migration table
	if len(ch.config.ClusterName) > 0 {
		tableExistenceQuery = fmt.Sprintf(`
			CREATE TABLE %s ON CLUSTER %s (
				version    Int64,
				dirty      UInt8,
				sequence   UInt64
			) Engine=%s`, ch.migrationTableReference(), ch.config.ClusterName, ch.config.MigrationsTableEngine)
	} else {
		tableExistenceQuery = fmt.Sprintf(`
			CREATE TABLE %s (
				version    Int64,
				dirty      UInt8,
				sequence   UInt64
			) Engine=%s`, ch.migrationTableReference(), ch.config.MigrationsTableEngine)
	}

	if strings.HasSuffix(ch.config.MigrationsTableEngine, "Tree") {
		tableExistenceQuery = fmt.Sprintf(`%s ORDER BY sequence`, tableExistenceQuery)
	}

	if _, err := ch.conn.Exec(tableExistenceQuery); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(tableExistenceQuery)}
	}
	return nil
}

func (ch *ClickHouse) Drop() (err error) {
	query := fmt.Sprintf("SHOW TABLES FROM %s", ch.dbReference())
	tables, err := ch.conn.Query(query)

	if err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}
	defer func() {
		if errClose := tables.Close(); errClose != nil {
			err = multierror.Append(err, errClose)
		}
	}()

	for tables.Next() {
		var table string
		if err := tables.Scan(&table); err != nil {
			return err
		}

		query = fmt.Sprintf("DROP TABLE IF EXISTS %s", ch.migrationTableReference())

		if _, err := ch.conn.Exec(query); err != nil {
			return &database.Error{OrigErr: err, Query: []byte(query)}
		}
	}
	if err := tables.Err(); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}

	return nil
}

func (ch *ClickHouse) Lock() error {
	if !ch.isLocked.CAS(false, true) {
		return database.ErrLocked
	}

	return nil
}
func (ch *ClickHouse) Unlock() error {
	if !ch.isLocked.CAS(true, false) {
		return database.ErrNotLocked
	}

	return nil
}
func (ch *ClickHouse) Close() error { return ch.conn.Close() }

// dbReference returns the escaped DB name
func (ch *ClickHouse) dbReference() string {
	return backtickEscape(ch.config.DatabaseName)
}

// dbAndTableName returns the fully qualified table name
// which includes the escaped DB name and escaped table name
func (ch *ClickHouse) migrationTableReference() string {
	return fmt.Sprintf(
		"%s.%s",
		ch.dbReference(),
		backtickEscape(ch.config.MigrationsTable),
	)
}

// backtickEscape returns the given string wrapped in backticks (`)
func backtickEscape(dbOrTableName string) string {
	return fmt.Sprintf("`%s`", dbOrTableName)
}
