package firebolt

import (
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
	"github.com/golang-migrate/migrate/v4/database/multistmt"
)

var (
	multiStmtDelimiter = []byte(";")

	DefaultMigrationsTable       = "schema_migrations"
	DefaultMultiStatementMaxSize = 10 * 1 << 20 // 10 MB

	ErrNilConfig = fmt.Errorf("no config")
)

type Config struct {
	DatabaseName          string
	MigrationsTable       string
	MultiStatementEnabled bool
	MultiStatementMaxSize int
}

func init() {
	database.Register("firebolt", &Firebolt{})
}

func WithInstance(conn *sql.DB, config *Config) (database.Driver, error) {
	if config == nil {
		return nil, ErrNilConfig
	}

	if err := conn.Ping(); err != nil {
		return nil, err
	}

	fb := &Firebolt{
		conn:   conn,
		config: config,
	}

	if err := fb.init(); err != nil {
		return nil, err
	}

	return fb, nil
}

type Firebolt struct {
	conn     *sql.DB
	config   *Config
	isLocked atomic.Bool
}

func (fb *Firebolt) Open(dsn string) (database.Driver, error) {
	purl, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}
	q := migrate.FilterCustomQuery(purl)
	// Rebuild DSN manually: url.URL.String() drops the "//" authority
	// separator when both Host and Path are empty, producing "firebolt:?..."
	// which the Firebolt SDK rejects. We also preserve raw (unencoded)
	// parameter values by splicing the original query string.
	cleanDSN := "firebolt://" + q.Path
	if q.RawQuery != "" {
		cleanDSN += "?" + q.RawQuery
	}
	conn, err := sql.Open("firebolt", cleanDSN)
	if err != nil {
		return nil, err
	}

	multiStatementMaxSize := DefaultMultiStatementMaxSize
	if s := purl.Query().Get("x-multi-statement-max-size"); len(s) > 0 {
		multiStatementMaxSize, err = strconv.Atoi(s)
		if err != nil {
			return nil, err
		}
	}

	dbName := strings.TrimPrefix(purl.Path, "/")

	fb = &Firebolt{
		conn: conn,
		config: &Config{
			MigrationsTable:       purl.Query().Get("x-migrations-table"),
			DatabaseName:          dbName,
			MultiStatementEnabled: purl.Query().Get("x-multi-statement") == "true",
			MultiStatementMaxSize: multiStatementMaxSize,
		},
	}

	if err := fb.init(); err != nil {
		return nil, err
	}

	return fb, nil
}

func (fb *Firebolt) init() error {
	if len(fb.config.MigrationsTable) == 0 {
		fb.config.MigrationsTable = DefaultMigrationsTable
	}

	if fb.config.MultiStatementMaxSize <= 0 {
		fb.config.MultiStatementMaxSize = DefaultMultiStatementMaxSize
	}

	return fb.ensureVersionTable()
}

func (fb *Firebolt) Run(r io.Reader) error {
	if fb.config.MultiStatementEnabled {
		var err error
		if e := multistmt.Parse(r, multiStmtDelimiter, fb.config.MultiStatementMaxSize, func(m []byte) bool {
			tq := strings.TrimSpace(string(m))
			if tq == "" {
				return true
			}
			if _, e := fb.conn.Exec(string(m)); e != nil {
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

	if _, err := fb.conn.Exec(string(migration)); err != nil {
		return database.Error{OrigErr: err, Err: "migration failed", Query: migration}
	}

	return nil
}

func (fb *Firebolt) Version() (int, bool, error) {
	var (
		version int
		dirty   bool
		query   = "SELECT version, dirty FROM " + quoteIdentifier(fb.config.MigrationsTable) + " ORDER BY sequence DESC LIMIT 1"
	)
	if err := fb.conn.QueryRow(query).Scan(&version, &dirty); err != nil {
		if err == sql.ErrNoRows {
			return database.NilVersion, false, nil
		}
		return 0, false, &database.Error{OrigErr: err, Query: []byte(query)}
	}
	return version, dirty, nil
}

func (fb *Firebolt) SetVersion(version int, dirty bool) error {
	tx, err := fb.conn.Begin()
	if err != nil {
		return err
	}

	query := "INSERT INTO " + quoteIdentifier(fb.config.MigrationsTable) + " (version, dirty, sequence) VALUES (?, ?, ?)"
	if _, err := tx.Exec(query, version, dirty, time.Now().UnixNano()); err != nil {
		if errRollback := tx.Rollback(); errRollback != nil {
			err = errors.Join(err, errRollback)
		}
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}

	return tx.Commit()
}

// ensureVersionTable checks if the migrations table exists and creates it if not.
// This function acquires and releases the lock internally.
func (fb *Firebolt) ensureVersionTable() (err error) {
	if err = fb.Lock(); err != nil {
		return err
	}

	defer func() {
		if e := fb.Unlock(); e != nil {
			err = errors.Join(err, e)
		}
	}()

	var table string
	query := "SELECT table_name FROM information_schema.tables WHERE table_name = '" + fb.config.MigrationsTable + "' AND table_type = 'BASE TABLE'"
	if err := fb.conn.QueryRow(query).Scan(&table); err != nil {
		if err != sql.ErrNoRows {
			return &database.Error{OrigErr: err, Query: []byte(query)}
		}
	} else {
		return nil
	}

	createQuery := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		version BIGINT,
		dirty BOOLEAN,
		sequence BIGINT
	)`, quoteIdentifier(fb.config.MigrationsTable))

	if _, err := fb.conn.Exec(createQuery); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(createQuery)}
	}
	return nil
}

func (fb *Firebolt) Drop() (err error) {
	query := "SELECT table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE'"
	tables, err := fb.conn.Query(query)
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

		query = "DROP TABLE IF EXISTS " + quoteIdentifier(table) + " CASCADE"
		if _, err := fb.conn.Exec(query); err != nil {
			return &database.Error{OrigErr: err, Query: []byte(query)}
		}
	}
	if err := tables.Err(); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}

	return nil
}

func (fb *Firebolt) Lock() error {
	if !fb.isLocked.CompareAndSwap(false, true) {
		return database.ErrLocked
	}

	return nil
}

func (fb *Firebolt) Unlock() error {
	if !fb.isLocked.CompareAndSwap(true, false) {
		return database.ErrNotLocked
	}

	return nil
}

func (fb *Firebolt) Close() error { return fb.conn.Close() }

func quoteIdentifier(name string) string {
	end := strings.IndexRune(name, 0)
	if end > -1 {
		name = name[:end]
	}
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}
