package ydb

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net/url"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/scheme"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
)

func init() {
	database.Register("ydb", &YDB{})
}

const (
	systemDirectory         = ".sys"
	systemMetadataDirectory = ".metadata"
	systemHealthDirectory   = ".sys_health"
	defaultMigrationsTable  = "schema_migrations"
	defaultLockTable        = "schema_lock"
	defaultStatementTimeout = 5 * time.Minute

	queryParamStatementTimeout          = "x-statement-timeout"
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
	ErrUnsupportedTLSVersion = fmt.Errorf("unsupported tls version: use 1.2 or 1.3")
)

type Config struct {
	MigrationsTable  string
	LockTable        string
	DatabaseName     string
	StatementTimeout time.Duration
}

type YDB struct {
	db       *ydb.Driver
	isLocked atomic.Bool
	config   *Config
	dropped  bool
}

// WithInstance initializes the migration tables using a native YDB driver.
// Ownership transfers on success: Close closes instance. On failure, the caller
// remains responsible for closing instance.
func WithInstance(instance *ydb.Driver, config *Config) (database.Driver, error) {
	if config == nil {
		return nil, ErrNilConfig
	}
	if instance == nil {
		return nil, errors.New("nil YDB driver")
	}
	cfg := *config
	if cfg.StatementTimeout < 0 {
		return nil, errors.New("statement timeout must be positive")
	}
	if cfg.StatementTimeout == 0 {
		cfg.StatementTimeout = defaultStatementTimeout
	}
	if cfg.DatabaseName == "" {
		cfg.DatabaseName = instance.Name()
	}
	if cfg.DatabaseName != instance.Name() {
		return nil, errors.New("database name does not match YDB driver")
	}
	if cfg.MigrationsTable == "" {
		cfg.MigrationsTable = defaultMigrationsTable
	}
	if cfg.LockTable == "" {
		cfg.LockTable = defaultLockTable
	}
	for _, name := range []string{cfg.MigrationsTable, cfg.LockTable} {
		if name == "." || name == ".." || strings.ContainsAny(name, "/`\\\x00") {
			return nil, fmt.Errorf("invalid migration table name %q", name)
		}
	}
	if cfg.MigrationsTable == cfg.LockTable {
		return nil, errors.New("migration and lock tables must differ")
	}
	db := &YDB{db: instance, config: &cfg}
	if err := db.ensureLockTable(); err != nil {
		return nil, err
	}
	if err := db.ensureVersionTable(); err != nil {
		return nil, err
	}
	return db, nil
}

func (y *YDB) Open(dsn string) (database.Driver, error) {
	purl, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}
	if purl.Path == "" {
		return nil, ErrNoDatabaseName
	}
	pquery, err := url.ParseQuery(purl.RawQuery)
	if err != nil {
		return nil, err
	}
	secure, err := parseBoolOption(pquery, queryParamUseGRPCS)
	if err != nil {
		return nil, err
	}
	purl.Scheme = "grpc"
	if secure {
		purl.Scheme = "grpcs"
	}
	purl = migrate.FilterCustomQuery(purl)
	credentials := y.parseCredentialsOptions(purl, pquery)
	tlsOptions, err := y.parseTLSOptions(purl, pquery)
	if err != nil {
		return nil, err
	}
	timeout := defaultStatementTimeout
	if pquery.Has(queryParamStatementTimeout) {
		millis, err := strconv.ParseInt(pquery.Get(queryParamStatementTimeout), 10, 64)
		if err != nil || millis <= 0 || millis > int64((1<<63-1)/time.Millisecond) {
			return nil, errors.New("x-statement-timeout must be a positive number of milliseconds")
		}
		timeout = time.Duration(millis) * time.Millisecond
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	nativeDriver, err := ydb.Open(ctx, purl.String(), append(tlsOptions, credentials)...)
	if err != nil {
		return nil, err
	}
	db, err := WithInstance(nativeDriver, &Config{
		MigrationsTable:  pquery.Get(queryParamMigrationsTable),
		LockTable:        pquery.Get(queryParamLockTable),
		DatabaseName:     purl.Path,
		StatementTimeout: timeout,
	})
	if err != nil {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), timeout)
		defer closeCancel()
		return nil, errors.Join(err, nativeDriver.Close(closeCtx))
	}
	return db, nil
}

func parseBoolOption(values url.Values, name string) (bool, error) {
	if !values.Has(name) {
		return false, nil
	}
	if values.Get(name) == "" {
		return true, nil
	}
	value, err := strconv.ParseBool(values.Get(name))
	if err != nil {
		return false, fmt.Errorf("invalid %s: %w", name, err)
	}
	return value, nil
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
	insecure, err := parseBoolOption(query, queryParamTLSInsecureSkipVerify)
	if err != nil {
		return nil, err
	}
	if insecure {
		options = append(options, ydb.WithTLSSInsecureSkipVerify())
	}
	if query.Has(queryParamTLSMinVersion) {
		switch query.Get(queryParamTLSMinVersion) {
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

func (y *YDB) context() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), y.config.StatementTimeout)
}

func (y *YDB) Close() error {
	ctx, cancel := y.context()
	defer cancel()
	return y.db.Close(ctx)
}

func quotePath(name string) string {
	return "`" + strings.ReplaceAll(strings.ReplaceAll(name, "\\", "\\\\"), "`", "\\`") + "`"
}

func (y *YDB) table(name string) string { return quotePath(path.Join(y.config.DatabaseName, name)) }

func (y *YDB) exec(sql string, opts ...query.ExecuteOption) error {
	ctx, cancel := y.context()
	defer cancel()
	return y.execContext(ctx, sql, opts...)
}

func (y *YDB) execContext(ctx context.Context, sql string, opts ...query.ExecuteOption) error {
	if err := y.db.Query().Exec(ctx, sql, opts...); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(sql)}
	}
	return nil
}

func (y *YDB) Run(migration io.Reader) error {
	raw, err := io.ReadAll(migration)
	if err != nil {
		return err
	}
	ctx, cancel := y.context()
	defer cancel()
	return y.db.Query().Do(ctx, func(ctx context.Context, session query.Session) error {
		if err := session.Exec(ctx, string(raw)); err != nil {
			// A migration can commit some statements before failing. database.Error
			// intentionally hides SDK retry classification (it has no Unwrap), so Do
			// may retry session acquisition but never a failed migration execution.
			return &database.Error{OrigErr: err, Err: "migration failed", Query: raw}
		}
		return nil
	}, query.WithIdempotent(false))
}

func (y *YDB) SetVersion(version int, dirty bool) error {
	sql := "DELETE FROM " + y.table(y.config.MigrationsTable) + ";"
	// Keep the dirty nil version after a failed first down migration (issue #330).
	// Encode the -1 sentinel as Uint64 and convert it back in Version to preserve
	// compatibility with the existing migration table schema.
	if version >= 0 || (version == database.NilVersion && dirty) {
		sql = "DECLARE $version AS Uint64; DECLARE $dirty AS Bool; " + sql +
			" INSERT INTO " + y.table(y.config.MigrationsTable) +
			" (version, dirty, created) VALUES ($version, $dirty, CurrentUtcTimestamp());"
	}
	ctx, cancel := y.context()
	defer cancel()
	err := y.db.Query().DoTx(ctx, func(ctx context.Context, tx query.TxActor) error {
		if version >= 0 || (version == database.NilVersion && dirty) {
			return tx.Exec(ctx, sql, query.WithParameters(
				ydb.ParamsBuilder().
					Param("$version").Uint64(uint64(version)).
					Param("$dirty").Bool(dirty).
					Build(),
			))
		}
		return tx.Exec(ctx, sql)
	}, query.WithIdempotent())
	if err != nil {
		return &database.Error{OrigErr: err, Query: []byte(sql)}
	}
	return nil
}

func (y *YDB) Version() (int, bool, error) {
	sql := "SELECT version, dirty FROM " + y.table(y.config.MigrationsTable) + " LIMIT 1"
	ctx, cancel := y.context()
	defer cancel()
	row, err := y.db.Query().QueryRow(ctx, sql, query.WithIdempotent())
	if errors.Is(err, query.ErrNoRows) {
		return database.NilVersion, false, nil
	}
	if err != nil {
		return 0, false, &database.Error{OrigErr: err, Query: []byte(sql)}
	}
	var version uint64
	var dirty bool
	if err := row.Scan(&version, &dirty); err != nil {
		return 0, false, &database.Error{OrigErr: err, Query: []byte(sql)}
	}
	return int(version), dirty, nil
}

type dropEntry struct {
	name string
	kind scheme.EntryType
}

// dropPlan validates the entire tree before deleting anything. Unsupported
// objects must not silently survive a successful Drop.
func (y *YDB) dropPlan(ctx context.Context, dir string, entries *[]dropEntry) error {
	listing, err := y.db.Scheme().ListDirectory(ctx, dir)
	if err != nil {
		return err
	}
	for _, child := range listing.Children {
		// YDB owns these database-root directories; they are not user schema.
		if dir == y.config.DatabaseName {
			switch child.Name {
			case systemDirectory, systemMetadataDirectory, systemHealthDirectory:
				continue
			}
		}
		name := path.Join(dir, child.Name)
		switch child.Type {
		case scheme.EntryDirectory, scheme.EntryColumnStore:
			if err := y.dropPlan(ctx, name, entries); err != nil {
				return err
			}
		case scheme.EntryTable, scheme.EntryColumnTable,
			scheme.EntryTopic, scheme.EntryPersQueueGroup,
			scheme.EntryExternalTable, scheme.EntryExternalDataSource:
		default:
			return fmt.Errorf("cannot drop unsupported YDB object %q (type %s)", name, child.Type)
		}
		*entries = append(*entries, dropEntry{name, child.Type})
	}
	return nil
}

func (y *YDB) Drop() error {
	ctx, cancel := y.context()
	defer cancel()
	var entries []dropEntry
	if err := y.dropPlan(ctx, y.config.DatabaseName, &entries); err != nil {
		return err
	}
	lockPath := path.Join(y.config.DatabaseName, y.config.LockTable)
	// External tables precede sources; all ordinary objects precede containers.
	// Preserve postorder for nested directories and delete the migration lock last.
	priority := func(e dropEntry) int {
		if e.name == lockPath {
			return 4
		}
		switch e.kind {
		case scheme.EntryExternalDataSource:
			return 1
		case scheme.EntryColumnStore:
			return 2
		case scheme.EntryDirectory:
			return 3
		default:
			return 0
		}
	}
	sort.SliceStable(entries, func(i, j int) bool { return priority(entries[i]) < priority(entries[j]) })
	for _, e := range entries {
		var err error
		switch e.kind {
		case scheme.EntryDirectory:
			err = y.db.Scheme().RemoveDirectory(ctx, e.name)
		case scheme.EntryTopic, scheme.EntryPersQueueGroup:
			err = y.db.Topic().Drop(ctx, e.name)
		case scheme.EntryExternalTable:
			err = y.execContext(ctx, "DROP EXTERNAL TABLE "+quotePath(e.name))
		case scheme.EntryExternalDataSource:
			err = y.execContext(ctx, "DROP EXTERNAL DATA SOURCE "+quotePath(e.name))
		case scheme.EntryColumnStore:
			err = y.execContext(ctx, "DROP TABLESTORE "+quotePath(e.name))
		default:
			err = y.execContext(ctx, "DROP TABLE "+quotePath(e.name))
		}
		if err != nil {
			return fmt.Errorf("drop %q: %w", e.name, err)
		}
	}
	y.dropped = true
	return nil
}

func (y *YDB) Lock() error {
	return database.CasRestoreOnErr(&y.isLocked, false, true, database.ErrLocked, func() error {
		aid, err := database.GenerateAdvisoryLockId(y.config.DatabaseName)
		if err != nil {
			return err
		}
		parameters := query.WithParameters(ydb.ParamsBuilder().Param("$id").Bytes([]byte(aid)).Build())
		// DoTx uses serializable read-write isolation. Do not retry an ambiguous
		// successful commit: a persistent lock has no expiration or owner recovery.
		ctx, cancel := y.context()
		defer cancel()
		err = y.db.Query().DoTx(ctx, func(ctx context.Context, tx query.TxActor) error {
			sql := "DECLARE $id AS String; SELECT lock_id FROM " + y.table(y.config.LockTable) + " WHERE lock_id = $id"
			_, err := tx.QueryRow(ctx, sql, parameters)
			if err == nil {
				return database.ErrLocked
			}
			if !errors.Is(err, query.ErrNoRows) {
				return err
			}
			sql = "DECLARE $id AS String; INSERT INTO " + y.table(y.config.LockTable) + " (lock_id) VALUES ($id)"
			if err := tx.Exec(ctx, sql, parameters); err != nil {
				return err
			}
			return nil
		}, query.WithIdempotent(false))
		if err != nil && !errors.Is(err, database.ErrLocked) {
			return &database.Error{OrigErr: err, Err: "failed to acquire migration lock"}
		}
		return err
	})
}

func (y *YDB) Unlock() error {
	return database.CasRestoreOnErr(&y.isLocked, true, false, database.ErrNotLocked, func() error {
		// Only a completed Drop justifies skipping the missing lock table.
		if y.dropped {
			return nil
		}
		aid, err := database.GenerateAdvisoryLockId(y.config.DatabaseName)
		if err != nil {
			return err
		}
		return y.exec(
			"DECLARE $id AS String; DELETE FROM "+y.table(y.config.LockTable)+" WHERE lock_id = $id",
			query.WithParameters(ydb.ParamsBuilder().Param("$id").Bytes([]byte(aid)).Build()),
			query.WithIdempotent(false),
		)
	})
}

func (y *YDB) ensureLockTable() error {
	return y.exec("CREATE TABLE IF NOT EXISTS " + y.table(y.config.LockTable) +
		" (lock_id String NOT NULL, PRIMARY KEY(lock_id))")
}

func (y *YDB) ensureVersionTable() (err error) {
	if err = y.Lock(); err != nil {
		return err
	}
	defer func() { err = errors.Join(err, y.Unlock()) }()
	return y.exec("CREATE TABLE IF NOT EXISTS " + y.table(y.config.MigrationsTable) +
		" (version Uint64 NOT NULL, dirty Bool NOT NULL, created Timestamp NOT NULL, PRIMARY KEY(version))")
}
