package turso

import (
	"database/sql"
	"errors"
	"fmt"
	"io"
	nurl "net/url"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
	_ "turso.tech/database/tursogo"
)

func init() {
	database.Register("turso", &Turso{})
}

var DefaultMigrationsTable = "schema_migrations"

var (
	ErrDatabaseDirty  = fmt.Errorf("database is dirty")
	ErrNilConfig      = fmt.Errorf("no config")
	ErrNoDatabaseName = fmt.Errorf("no database name")
)

// Config is used with WithInstance to configure the turso migration driver.
//
// Turso-specific fields (BusyTimeout, Experimental, EncryptionCipher,
// EncryptionHexkey, Vfs, AsyncIO) are only meaningful when used via Open(url).
// When calling WithInstance, these fields are advisory — the caller has already
// constructed the *sql.DB with the desired DSN, and the driver does not
// re-apply them to the connection.
type Config struct {
	MigrationsTable string
	DatabaseName    string
	NoTxWrap        bool

	// Turso-specific. All optional — zero values mean "use turso default".
	// These fields control the DSN parameters passed to turso-go when
	// Open(url) constructs the underlying *sql.DB. They are ignored by
	// WithInstance since the connection is already established.

	// BusyTimeout in milliseconds. 0 = use turso default (5000ms). -1 = disabled.
	BusyTimeout int
	// Experimental is a comma-separated list of turso-go experimental features.
	// Known values: encryption, custom_types, index_method, fts, mvcc.
	Experimental string
	// EncryptionCipher is the cipher for encryption at rest (e.g. "aegis256").
	// Requires "encryption" in Experimental.
	EncryptionCipher string
	// EncryptionHexkey is the hex-encoded encryption key (64 hex chars for AEGIS-256).
	EncryptionHexkey string
	// Vfs selects the VFS backend: "memory", "syscall", "io_uring", "experimental_win_iocp".
	Vfs string
	// AsyncIO enables async IO mode in turso-go.
	AsyncIO bool
}

type Turso struct {
	db       *sql.DB
	isLocked atomic.Bool

	config *Config
}

func WithInstance(instance *sql.DB, config *Config) (database.Driver, error) {
	if config == nil {
		return nil, ErrNilConfig
	}

	if err := instance.Ping(); err != nil {
		return nil, err
	}

	if len(config.MigrationsTable) == 0 {
		config.MigrationsTable = DefaultMigrationsTable
	}

	mx := &Turso{
		db:     instance,
		config: config,
	}
	if err := mx.ensureVersionTable(); err != nil {
		return nil, err
	}
	return mx, nil
}

// ensureVersionTable checks if versions table exists and, if not, creates it.
// Note that this function locks the database, which deviates from the usual
// convention of "caller locks" in the Turso type.
func (m *Turso) ensureVersionTable() (err error) {
	if err = m.Lock(); err != nil {
		return err
	}

	defer func() {
		if e := m.Unlock(); e != nil {
			err = errors.Join(err, e)
		}
	}()

	// turso-go ExecContext supports multi-statement execution natively
	// (verified in driver_db.go:221-278), so this works without splitting.
	query := fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %s (version uint64,dirty bool);
  CREATE UNIQUE INDEX IF NOT EXISTS version_unique ON %s (version);
  `, m.config.MigrationsTable, m.config.MigrationsTable)

	if _, err := m.db.Exec(query); err != nil {
		return err
	}
	return nil
}

func (m *Turso) Open(url string) (database.Driver, error) {
	purl, err := nurl.Parse(url)
	if err != nil {
		return nil, err
	}

	// Build the turso-go DSN from the URL.
	// 1. Strip x-* params (consumed by the migration driver).
	// 2. Translate x-* params to turso-go DSN params.
	// 3. Append translated params to the filtered URL.
	qv := purl.Query()

	// Parse migration-driver params first.
	migrationsTable := qv.Get("x-migrations-table")
	if len(migrationsTable) == 0 {
		migrationsTable = DefaultMigrationsTable
	}

	noTxWrap := false
	if v := qv.Get("x-no-tx-wrap"); v != "" {
		noTxWrap, err = strconv.ParseBool(v)
		if err != nil {
			return nil, fmt.Errorf("x-no-tx-wrap: %s", err)
		}
	}

	// Parse turso-specific params from x-* query parameters.
	busyTimeout := 0
	if v := qv.Get("x-busy-timeout"); v != "" {
		busyTimeout, err = strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("x-busy-timeout: %s", err)
		}
	}

	experimental := qv.Get("x-experimental")
	encryptionCipher := qv.Get("x-encryption-cipher")
	encryptionHexkey := qv.Get("x-encryption-hexkey")
	vfs := qv.Get("x-vfs")

	asyncIO := false
	if v := qv.Get("x-async"); v != "" {
		asyncIO, err = strconv.ParseBool(v)
		if err != nil {
			return nil, fmt.Errorf("x-async: %s", err)
		}
	}

	// Build the turso-go DSN: strip the scheme, strip x-* params,
	// then append translated turso-go params.
	filtered := migrate.FilterCustomQuery(purl)
	dbfile := strings.Replace(filtered.String(), "turso://", "", 1)

	// Build additional turso-go DSN query parameters.
	// turso-go DSN format: <path>?param1=val1&param2=val2
	dsnParams := make(nurl.Values)
	if experimental != "" {
		dsnParams.Set("experimental", experimental)
	}
	if encryptionCipher != "" {
		dsnParams.Set("encryption_cipher", encryptionCipher)
	}
	if encryptionHexkey != "" {
		dsnParams.Set("encryption_hexkey", encryptionHexkey)
	}
	if busyTimeout != 0 {
		dsnParams.Set("_busy_timeout", strconv.Itoa(busyTimeout))
	}
	// Verified: turso-go uses "vfs" without underscore prefix (driver_db.go:638).
	if vfs != "" {
		dsnParams.Set("vfs", vfs)
	}
	if asyncIO {
		dsnParams.Set("async", "1")
	}

	// Merge turso-go params into the DSN.
	if len(dsnParams) > 0 {
		if strings.Contains(dbfile, "?") {
			dbfile += "&" + dsnParams.Encode()
		} else {
			dbfile += "?" + dsnParams.Encode()
		}
	}

	db, err := sql.Open("turso", dbfile)
	if err != nil {
		return nil, err
	}

	mx, err := WithInstance(db, &Config{
		DatabaseName:     purl.Path,
		MigrationsTable:  migrationsTable,
		NoTxWrap:         noTxWrap,
		BusyTimeout:      busyTimeout,
		Experimental:     experimental,
		EncryptionCipher: encryptionCipher,
		EncryptionHexkey: encryptionHexkey,
		Vfs:              vfs,
		AsyncIO:          asyncIO,
	})
	if err != nil {
		return nil, err
	}
	return mx, nil
}

func (m *Turso) Close() error {
	return m.db.Close()
}

func (m *Turso) Drop() (err error) {
	// Verified: turso-go supports sqlite_master (SQLite compatibility).
	query := `SELECT name FROM sqlite_master WHERE type = 'table';`
	tables, err := m.db.Query(query)
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
		if len(tableName) > 0 {
			tableNames = append(tableNames, tableName)
		}
	}
	if err := tables.Err(); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}

	if len(tableNames) > 0 {
		for _, t := range tableNames {
			query := "DROP TABLE " + t
			err = m.executeQuery(query)
			if err != nil {
				return &database.Error{OrigErr: err, Query: []byte(query)}
			}
		}
		query := "VACUUM"
		_, err = m.db.Query(query)
		if err != nil {
			return &database.Error{OrigErr: err, Query: []byte(query)}
		}
	}

	return nil
}

func (m *Turso) Lock() error {
	if !m.isLocked.CompareAndSwap(false, true) {
		return database.ErrLocked
	}
	return nil
}

func (m *Turso) Unlock() error {
	if !m.isLocked.CompareAndSwap(true, false) {
		return database.ErrNotLocked
	}
	return nil
}

func (m *Turso) Run(migration io.Reader) error {
	migr, err := io.ReadAll(migration)
	if err != nil {
		return err
	}
	query := string(migr[:])

	if m.config.NoTxWrap {
		return m.executeQueryNoTx(query)
	}
	return m.executeQuery(query)
}

func (m *Turso) executeQuery(query string) error {
	tx, err := m.db.Begin()
	if err != nil {
		return &database.Error{OrigErr: err, Err: "transaction start failed"}
	}
	if _, err := tx.Exec(query); err != nil {
		if errRollback := tx.Rollback(); errRollback != nil {
			err = errors.Join(err, errRollback)
		}
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}
	if err := tx.Commit(); err != nil {
		return &database.Error{OrigErr: err, Err: "transaction commit failed"}
	}
	return nil
}

func (m *Turso) executeQueryNoTx(query string) error {
	if _, err := m.db.Exec(query); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}
	return nil
}

func (m *Turso) SetVersion(version int, dirty bool) error {
	tx, err := m.db.Begin()
	if err != nil {
		return &database.Error{OrigErr: err, Err: "transaction start failed"}
	}

	query := "DELETE FROM " + m.config.MigrationsTable
	if _, err := tx.Exec(query); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}

	// Also re-write the schema version for nil dirty versions to prevent
	// empty schema version for failed down migration on the first migration
	// See: https://github.com/golang-migrate/migrate/issues/330
	if version >= 0 || (version == database.NilVersion && dirty) {
		query := fmt.Sprintf(`INSERT INTO %s (version, dirty) VALUES (?, ?)`, m.config.MigrationsTable)
		if _, err := tx.Exec(query, version, dirty); err != nil {
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

func (m *Turso) Version() (version int, dirty bool, err error) {
	query := "SELECT version, dirty FROM " + m.config.MigrationsTable + " LIMIT 1"
	err = m.db.QueryRow(query).Scan(&version, &dirty)
	if err != nil {
		return database.NilVersion, false, nil
	}
	return version, dirty, nil
}
