package ydb

import (
	"context"
	"crypto/tls"
	"database/sql"
	"fmt"
	"io"
	"net/url"
	"sync/atomic"

	"github.com/hashicorp/go-multierror"
	"github.com/ydb-platform/ydb-go-sdk/v3"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
)

func init() {
	database.Register("ydb", &YDB{})
}

const (
	defaultMigrationsTable = "schema_migrations"
	defaultLockTable       = "schema_lock"

	queryParamAuthToken                 = "x-auth-token"
	queryParamMigrationsTable           = "x-migrations-table"
	queryParamLockTable                 = "x-lock-table"
	queryParamUseGRPCS                  = "x-use-grpcs"
	queryParamTLSCertificateAuthorities = "x-tls-ca"
	queryParamTLSInsecureSkipVerify     = "x-tls-insecure-skip-verify"
	queryParamTLSMinVersion             = "x-tls-min-version"
)

var (
	ErrNilConfig             = fmt.Errorf("no config")
	ErrNoDatabaseName        = fmt.Errorf("no database name")
	ErrUnsupportedTLSVersion = fmt.Errorf("unsupported tls version: use 1.0, 1.1, 1,2 or 1.3")
)

type Config struct {
	MigrationsTable string
	LockTable       string
}

type YDB struct {
	// locking and unlocking need to use the same connection
	conn     *sql.Conn
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
		config.MigrationsTable = defaultMigrationsTable
	}

	if len(config.LockTable) == 0 {
		config.LockTable = defaultLockTable
	}

	conn, err := instance.Conn(context.TODO())
	if err != nil {
		return nil, err
	}

	db := &YDB{
		conn:   conn,
		db:     instance,
		config: config,
	}
	if err = db.ensureLockTable(); err != nil {
		return nil, err
	}
	if err = db.ensureVersionTable(); err != nil {
		return nil, err
	}
	return db, nil
}

func (y *YDB) Open(dsn string) (database.Driver, error) {
	purl, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}

	if len(purl.Path) == 0 {
		return nil, ErrNoDatabaseName
	}

	pquery, err := url.ParseQuery(purl.RawQuery)
	if err != nil {
		return nil, err
	}

	switch {
	case pquery.Has(queryParamUseGRPCS):
		purl.Scheme = "grpcs"
	default:
		purl.Scheme = "grpc"
	}

	purl = migrate.FilterCustomQuery(purl)

	credentials := y.parseCredentialsOptions(purl, pquery)
	tlsOptions, err := y.parseTLSOptions(purl, pquery)
	if err != nil {
		return nil, err
	}

	nativeDriver, err := ydb.Open(context.TODO(), purl.String(), append(tlsOptions, credentials)...)
	if err != nil {
		return nil, err
	}

	connector, err := ydb.Connector(nativeDriver,
		ydb.WithQueryService(true),
	)
	if err != nil {
		return nil, err
	}

	db, err := WithInstance(sql.OpenDB(connector), &Config{
		MigrationsTable: pquery.Get(queryParamMigrationsTable),
		LockTable:       pquery.Get(queryParamLockTable),
	})
	if err != nil {
		return nil, err
	}

	return db, nil
}

func (y *YDB) parseCredentialsOptions(url *url.URL, query url.Values) (credentials ydb.Option) {
	switch {
	case query.Has(queryParamAuthToken):
		credentials = ydb.WithAccessTokenCredentials(query.Get(queryParamAuthToken))
	case url.User != nil:
		user := url.User.Username()
		password, _ := url.User.Password()
		credentials = ydb.WithStaticCredentials(user, password)
	default:
		credentials = ydb.WithAnonymousCredentials()
	}
	url.User = nil
	return credentials
}

func (y *YDB) parseTLSOptions(_ *url.URL, query url.Values) (options []ydb.Option, err error) {
	if query.Has(queryParamTLSCertificateAuthorities) {
		options = append(options, ydb.WithCertificatesFromFile(query.Get(queryParamTLSCertificateAuthorities)))
	}
	if query.Has(queryParamTLSInsecureSkipVerify) {
		options = append(options, ydb.WithTLSSInsecureSkipVerify())
	}
	if query.Has(queryParamTLSMinVersion) {
		switch query.Get(queryParamTLSMinVersion) {
		case "1.0":
			options = append(options, ydb.WithMinTLSVersion(tls.VersionTLS10))
		case "1.1":
			options = append(options, ydb.WithMinTLSVersion(tls.VersionTLS11))
		case "1.2":
			options = append(options, ydb.WithMinTLSVersion(tls.VersionTLS12))
		case "1.3":
			options = append(options, ydb.WithMinTLSVersion(tls.VersionTLS13))
		default:
			return nil, ErrUnsupportedTLSVersion
		}
	}
	return options, nil
}

func (y *YDB) Close() error {
	connErr := y.conn.Close()
	var dbErr error
	if y.db != nil {
		dbErr = y.db.Close()
	}
	if connErr != nil || dbErr != nil {
		return fmt.Errorf("conn: %v, db: %v", connErr, dbErr)
	}
	return nil
}

func (y *YDB) Run(migration io.Reader) error {
	rawMigrations, err := io.ReadAll(migration)
	if err != nil {
		return err
	}

	if _, err = y.conn.ExecContext(ydb.WithQueryMode(context.TODO(), ydb.SchemeQueryMode), string(rawMigrations)); err != nil {
		return database.Error{OrigErr: err, Err: "migration failed", Query: rawMigrations}
	}
	return nil
}

func (y *YDB) SetVersion(version int, dirty bool) error {
	deleteVersionQuery := fmt.Sprintf(`
		DELETE FROM %s 
	`, y.config.MigrationsTable)

	insertVersionQuery := fmt.Sprintf(`
		INSERT INTO %s (version, dirty, created) VALUES (%d, %t, CurrentUtcTimestamp())
	`, y.config.MigrationsTable, version, dirty)

	tx, err := y.conn.BeginTx(context.TODO(), &sql.TxOptions{})
	if err != nil {
		return &database.Error{OrigErr: err, Err: "transaction start failed"}
	}

	if _, err := tx.Exec(deleteVersionQuery); err != nil {
		if errRollback := tx.Rollback(); errRollback != nil {
			err = multierror.Append(err, errRollback)
		}
		return &database.Error{OrigErr: err, Query: []byte(deleteVersionQuery)}
	}

	// Also re-write the schema version for nil dirty versions to prevent
	// empty schema version for failed down migration on the first migration
	// See: https://github.com/golang-migrate/migrate/issues/330
	if version >= 0 || (version == database.NilVersion && dirty) {
		if _, err := tx.Exec(insertVersionQuery, version, dirty); err != nil {
			if errRollback := tx.Rollback(); errRollback != nil {
				err = multierror.Append(err, errRollback)
			}
			return &database.Error{OrigErr: err, Query: []byte(insertVersionQuery)}
		}
	}

	if err := tx.Commit(); err != nil {
		return &database.Error{OrigErr: err, Err: "transaction commit failed"}
	}
	return err
}

func (y *YDB) Version() (version int, dirty bool, err error) {
	getVersionQuery := fmt.Sprintf(`
		SELECT version, dirty FROM %s LIMIT 1
	`, y.config.MigrationsTable)

	var v uint64
	err = y.conn.QueryRowContext(context.TODO(), getVersionQuery).Scan(&v, &dirty)
	switch {
	case err == sql.ErrNoRows:
		return database.NilVersion, false, nil
	case err != nil:
		return 0, false, &database.Error{OrigErr: err, Query: []byte(getVersionQuery)}
	default:
		return int(v), dirty, err
	}
}

func (y *YDB) Drop() (err error) {
	listQuery := "SELECT DISTINCT Path FROM `.sys/partition_stats` WHERE Path NOT LIKE '%/.sys%'"
	rs, err := y.conn.QueryContext(context.TODO(), listQuery)
	if err != nil {
		return &database.Error{OrigErr: err, Query: []byte(listQuery)}
	}
	defer func() {
		if closeErr := rs.Close(); closeErr != nil {
			err = multierror.Append(err, closeErr)
		}
	}()

	paths := make([]string, 0)
	for rs.Next() {
		var path string
		if err = rs.Scan(&path); err != nil {
			return err
		}
		if len(path) != 0 {
			paths = append(paths, path)
		}
	}
	if err = rs.Err(); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(listQuery)}
	}

	for _, path := range paths {
		dropQuery := fmt.Sprintf("DROP TABLE IF EXISTS `%s`", path)
		if _, err = y.conn.ExecContext(ydb.WithQueryMode(context.TODO(), ydb.SchemeQueryMode), dropQuery); err != nil {
			return &database.Error{OrigErr: err, Query: []byte(dropQuery)}
		}
	}
	return nil
}

func (y *YDB) Lock() error {
	if !y.isLocked.CompareAndSwap(false, true) {
		return database.ErrLocked
	}
	return nil
}

func (y *YDB) Unlock() error {
	if !y.isLocked.CompareAndSwap(true, false) {
		return database.ErrNotLocked
	}
	return nil
}

// ensureLockTable checks if lock table exists and, if not, creates it.
func (y *YDB) ensureLockTable() (err error) {
	createLockTableQuery := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			lock_id String NOT NULL,
			PRIMARY KEY(lock_id)
		)
	`, y.config.LockTable)
	if _, err = y.conn.ExecContext(ydb.WithQueryMode(context.TODO(), ydb.SchemeQueryMode), createLockTableQuery); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(createLockTableQuery)}
	}
	return nil
}

// ensureVersionTable checks if versions table exists and, if not, creates it.
func (y *YDB) ensureVersionTable() (err error) {
	if err = y.Lock(); err != nil {
		return err
	}

	defer func() {
		if unlockErr := y.Unlock(); unlockErr != nil {
			if err == nil {
				err = unlockErr
			} else {
				err = multierror.Append(err, unlockErr)
			}
		}
	}()

	createVersionTableQuery := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			version Uint64 NOT NULL,
			dirty Bool NOT NULL,
			created Timestamp NOT NULL,
			PRIMARY KEY(version)
		)
	`, y.config.MigrationsTable)
	if _, err = y.conn.ExecContext(ydb.WithQueryMode(context.TODO(), ydb.SchemeQueryMode), createVersionTableQuery); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(createVersionTableQuery)}
	}
	return nil
}
