package clickhouse

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
	"github.com/golang-migrate/migrate/v4/database/multistmt"
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
	opts, err := optionsFromURL(purl)
	if err != nil {
		return nil, err
	}
	conn := clickhouse.OpenDB(opts)

	multiStatementMaxSize := DefaultMultiStatementMaxSize
	if s := purl.Query().Get("x-multi-statement-max-size"); len(s) > 0 {
		multiStatementMaxSize, err = strconv.Atoi(s)
		if err != nil {
			return nil, err
		}
	}

	migrationsTableEngine := DefaultMigrationsTableEngine
	if s := purl.Query().Get("x-migrations-table-engine"); len(s) > 0 {
		migrationsTableEngine = s
	}

	ch = &ClickHouse{
		conn: conn,
		config: &Config{
			MigrationsTable:       purl.Query().Get("x-migrations-table"),
			MigrationsTableEngine: migrationsTableEngine,
			DatabaseName:          purl.Query().Get("database"),
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

// optionsFromURL builds the clickhouse-go options for a migrate URL. It
// translates the connection parameters of clickhouse-go v1, which this driver
// used before, and sets up JWT authentication when x-jwt-token or
// x-jwt-token-file is present.
func optionsFromURL(purl *url.URL) (*clickhouse.Options, error) {
	q := migrate.FilterCustomQuery(purl)
	if err := translateV1Params(q); err != nil {
		return nil, err
	}
	q.Scheme = "tcp"
	opts, err := clickhouse.ParseDSN(q.String())
	if err != nil {
		return nil, err
	}

	token := purl.Query().Get("x-jwt-token")
	tokenFile := purl.Query().Get("x-jwt-token-file")
	if token == "" && tokenFile == "" {
		return opts, nil
	}
	if token != "" && tokenFile != "" {
		return nil, errors.New("clickhouse: set only one of x-jwt-token and x-jwt-token-file")
	}
	if opts.Auth.Username != "" || opts.Auth.Password != "" {
		return nil, errors.New("clickhouse: JWT authentication cannot be combined with a username or password")
	}
	if token != "" {
		opts.GetJWT = func(context.Context) (string, error) { return token, nil }
		return opts, nil
	}
	// Read on every new connection, so that a token the platform renews in
	// the file is picked up during a long migration.
	opts.GetJWT = func(context.Context) (string, error) {
		b, err := os.ReadFile(tokenFile)
		if err != nil {
			return "", fmt.Errorf("clickhouse: read x-jwt-token-file: %w", err)
		}
		token := strings.TrimSpace(string(b))
		if token == "" {
			return "", fmt.Errorf("clickhouse: x-jwt-token-file %s is empty", tokenFile)
		}
		return token, nil
	}
	return opts, nil
}

// translateV1Params rewrites connection parameters that clickhouse-go v1
// accepted into their clickhouse-go v2 form. clickhouse-go v2 sends any
// parameter it does not know to the server as a setting, which the server
// rejects, so v1 parameters without a v2 equivalent are removed.
func translateV1Params(u *url.URL) error {
	q := u.Query()
	if q.Has("tls_config") {
		return errors.New("clickhouse: tls_config is no longer supported, use secure, skip_verify and tls_server_name")
	}
	// v1 tuning knobs with no v2 equivalent. pool_size sized a byte buffer
	// pool in v1, not the connection pool.
	for _, name := range []string{"block_size", "no_delay", "pool_size", "write_timeout"} {
		q.Del(name)
	}
	// v1 read timeouts in seconds, v2 reads Go durations.
	if q.Has("read_timeout") {
		q.Set("read_timeout", secondsToDuration(q.Get("read_timeout")))
	}
	if q.Has("timeout") {
		if !q.Has("dial_timeout") {
			q.Set("dial_timeout", secondsToDuration(q.Get("timeout")))
		}
		q.Del("timeout")
	}
	if q.Get("connection_open_strategy") == "time_random" {
		q.Set("connection_open_strategy", "random")
	}
	// v2 takes additional hosts as a comma separated host list.
	if altHosts := q.Get("alt_hosts"); altHosts != "" {
		for _, host := range strings.Split(altHosts, ",") {
			if host != "" {
				u.Host += "," + host
			}
		}
	}
	q.Del("alt_hosts")
	u.RawQuery = q.Encode()
	return nil
}

// secondsToDuration turns a number of seconds into a Go duration string and
// leaves any other value, such as one that already has a unit, unchanged.
func secondsToDuration(value string) string {
	seconds, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return value
	}
	return time.Duration(seconds * float64(time.Second)).String()
}

func (ch *ClickHouse) init() error {
	if len(ch.config.DatabaseName) == 0 {
		if err := ch.conn.QueryRow("SELECT currentDatabase()").Scan(&ch.config.DatabaseName); err != nil {
			return err
		}
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
		query   = "SELECT version, dirty FROM `" + ch.config.MigrationsTable + "` ORDER BY sequence DESC LIMIT 1"
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

	query := "INSERT INTO " + ch.config.MigrationsTable + " (version, dirty, sequence) VALUES (?, ?, ?)"
	if _, err := tx.Exec(query, version, bool(dirty), time.Now().UnixNano()); err != nil {
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
			err = errors.Join(err, e)
		}
	}()

	var (
		table string
		query = "SHOW TABLES FROM " + quoteIdentifier(ch.config.DatabaseName) + " LIKE '" + ch.config.MigrationsTable + "'"
	)
	// check if migration table exists
	if err := ch.conn.QueryRow(query).Scan(&table); err != nil {
		if err != sql.ErrNoRows {
			return &database.Error{OrigErr: err, Query: []byte(query)}
		}
	} else {
		return nil
	}

	// if not, create the empty migration table
	if len(ch.config.ClusterName) > 0 {
		query = fmt.Sprintf(`
			CREATE TABLE %s ON CLUSTER %s (
				version    Int64,
				dirty      UInt8,
				sequence   UInt64
			) Engine=%s`, ch.config.MigrationsTable, ch.config.ClusterName, ch.config.MigrationsTableEngine)
	} else {
		query = fmt.Sprintf(`
			CREATE TABLE %s (
				version    Int64,
				dirty      UInt8,
				sequence   UInt64
			) Engine=%s`, ch.config.MigrationsTable, ch.config.MigrationsTableEngine)
	}

	if strings.HasSuffix(ch.config.MigrationsTableEngine, "Tree") {
		query = fmt.Sprintf(`%s ORDER BY sequence`, query)
	}

	if _, err := ch.conn.Exec(query); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}
	return nil
}

func (ch *ClickHouse) Drop() (err error) {
	query := "SHOW TABLES FROM " + quoteIdentifier(ch.config.DatabaseName)
	tables, err := ch.conn.Query(query)

	if err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}
	defer func() {
		if errClose := tables.Close(); errClose != nil {
			err = errors.Join(err, errClose)
		}
	}()

	for tables.Next() {
		var table string
		if err := tables.Scan(&table); err != nil {
			return err
		}

		query = "DROP TABLE IF EXISTS " + quoteIdentifier(ch.config.DatabaseName) + "." + quoteIdentifier(table)

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
	if !ch.isLocked.CompareAndSwap(false, true) {
		return database.ErrLocked
	}

	return nil
}
func (ch *ClickHouse) Unlock() error {
	if !ch.isLocked.CompareAndSwap(true, false) {
		return database.ErrNotLocked
	}

	return nil
}
func (ch *ClickHouse) Close() error { return ch.conn.Close() }

// Copied from lib/pq implementation: https://github.com/lib/pq/blob/v1.9.0/conn.go#L1611
func quoteIdentifier(name string) string {
	end := strings.IndexRune(name, 0)
	if end > -1 {
		name = name[:end]
	}
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}
