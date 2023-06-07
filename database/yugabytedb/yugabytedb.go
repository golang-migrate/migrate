package yugabytedb

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"net/url"
	"regexp"
	"strconv"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
	"github.com/hashicorp/go-multierror"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgerrcode"
	"github.com/lib/pq"
	"go.uber.org/atomic"
)

const (
	DefaultMaxRetryInterval    = time.Second * 15
	DefaultMaxRetryElapsedTime = time.Second * 30
	DefaultMaxRetries          = 10
	DefaultMigrationsTable     = "migrations"
	DefaultLockTable           = "migrations_locks"
)

var (
	ErrNilConfig          = errors.New("no config")
	ErrNoDatabaseName     = errors.New("no database name")
	ErrMaxRetriesExceeded = errors.New("max retries exceeded")
)

func init() {
	db := YugabyteDB{}
	database.Register("yugabyte", &db)
	database.Register("yugabytedb", &db)
	database.Register("ysql", &db)
}

type Config struct {
	MigrationsTable     string
	LockTable           string
	ForceLock           bool
	DatabaseName        string
	MaxRetryInterval    time.Duration
	MaxRetryElapsedTime time.Duration
	MaxRetries          int
}

type YugabyteDB struct {
	db       *sql.DB
	isLocked atomic.Bool

	// Open and WithInstance need to guarantee that config is never nil
	config *Config
}

func WithInstance(instance *sql.DB, config *Config) (database.Driver, error) {
	if config == nil {
		return nil, ErrNilConfig
	}

	if err := instance.Ping(); err != nil {
		return nil, err
	}

	if config.DatabaseName == "" {
		query := `SELECT current_database()`
		var databaseName string
		if err := instance.QueryRow(query).Scan(&databaseName); err != nil {
			return nil, &database.Error{OrigErr: err, Query: []byte(query)}
		}

		if len(databaseName) == 0 {
			return nil, ErrNoDatabaseName
		}

		config.DatabaseName = databaseName
	}

	if len(config.MigrationsTable) == 0 {
		config.MigrationsTable = DefaultMigrationsTable
	}

	if len(config.LockTable) == 0 {
		config.LockTable = DefaultLockTable
	}

	if config.MaxRetryInterval == 0 {
		config.MaxRetryInterval = DefaultMaxRetryInterval
	}

	if config.MaxRetryElapsedTime == 0 {
		config.MaxRetryElapsedTime = DefaultMaxRetryElapsedTime
	}

	if config.MaxRetries == 0 {
		config.MaxRetries = DefaultMaxRetries
	}

	px := &YugabyteDB{
		db:     instance,
		config: config,
	}

	// ensureVersionTable is a locking operation, so we need to ensureLockTable before we ensureVersionTable.
	if err := px.ensureLockTable(); err != nil {
		return nil, err
	}

	if err := px.ensureVersionTable(); err != nil {
		return nil, err
	}

	return px, nil
}

func (c *YugabyteDB) Open(dbURL string) (database.Driver, error) {
	purl, err := url.Parse(dbURL)
	if err != nil {
		return nil, err
	}

	// As YugabyteDB uses the postgres protocol, and 'postgres' is already a registered database, we need to replace the
	// connect prefix, with the actual protocol, so that the library can differentiate between the implementations
	re := regexp.MustCompile("^(yugabyte(db)?|ysql)")
	connectString := re.ReplaceAllString(migrate.FilterCustomQuery(purl).String(), "postgres")

	db, err := sql.Open("postgres", connectString)
	if err != nil {
		return nil, err
	}

	migrationsTable := purl.Query().Get("x-migrations-table")
	if len(migrationsTable) == 0 {
		migrationsTable = DefaultMigrationsTable
	}

	lockTable := purl.Query().Get("x-lock-table")
	if len(lockTable) == 0 {
		lockTable = DefaultLockTable
	}

	forceLockQuery := purl.Query().Get("x-force-lock")
	forceLock, err := strconv.ParseBool(forceLockQuery)
	if err != nil {
		forceLock = false
	}

	maxIntervalStr := purl.Query().Get("x-max-retry-interval")
	maxInterval, err := time.ParseDuration(maxIntervalStr)
	if err != nil {
		maxInterval = DefaultMaxRetryInterval
	}

	maxElapsedTimeStr := purl.Query().Get("x-max-retry-elapsed-time")
	maxElapsedTime, err := time.ParseDuration(maxElapsedTimeStr)
	if err != nil {
		maxElapsedTime = DefaultMaxRetryElapsedTime
	}

	maxRetriesStr := purl.Query().Get("x-max-retries")
	maxRetries, err := strconv.Atoi(maxRetriesStr)
	if err != nil {
		maxRetries = DefaultMaxRetries
	}

	px, err := WithInstance(db, &Config{
		DatabaseName:        purl.Path,
		MigrationsTable:     migrationsTable,
		LockTable:           lockTable,
		ForceLock:           forceLock,
		MaxRetryInterval:    maxInterval,
		MaxRetryElapsedTime: maxElapsedTime,
		MaxRetries:          maxRetries,
	})
	if err != nil {
		return nil, err
	}

	return px, nil
}

func (c *YugabyteDB) Close() error {
	return c.db.Close()
}

// Locking is done manually with a separate lock table. Implementing advisory locks in YugabyteDB is being discussed
// See: https://github.com/yugabyte/yugabyte-db/issues/3642
func (c *YugabyteDB) Lock() error {
	return database.CasRestoreOnErr(&c.isLocked, false, true, database.ErrLocked, func() (err error) {
		return c.doTxWithRetry(context.Background(), &sql.TxOptions{Isolation: sql.LevelSerializable}, func(tx *sql.Tx) (err error) {
			aid, err := database.GenerateAdvisoryLockId(c.config.DatabaseName)
			if err != nil {
				return err
			}

			query := "SELECT * FROM " + c.config.LockTable + " WHERE lock_id = $1"
			rows, err := tx.Query(query, aid)
			if err != nil {
				return database.Error{OrigErr: err, Err: "failed to fetch migration lock", Query: []byte(query)}
			}
			defer func() {
				if errClose := rows.Close(); errClose != nil {
					err = multierror.Append(err, errClose)
				}
			}()

			// If row exists at all, lock is present
			locked := rows.Next()
			if locked && !c.config.ForceLock {
				return database.ErrLocked
			}

			query = "INSERT INTO " + c.config.LockTable + " (lock_id) VALUES ($1)"
			if _, err := tx.Exec(query, aid); err != nil {
				return database.Error{OrigErr: err, Err: "failed to set migration lock", Query: []byte(query)}
			}

			return nil
		})
	})
}

// Locking is done manually with a separate lock table. Implementing advisory locks in YugabyteDB is being discussed
// See: https://github.com/yugabyte/yugabyte-db/issues/3642
func (c *YugabyteDB) Unlock() error {
	return database.CasRestoreOnErr(&c.isLocked, true, false, database.ErrNotLocked, func() (err error) {
		aid, err := database.GenerateAdvisoryLockId(c.config.DatabaseName)
		if err != nil {
			return err
		}

		// In the event of an implementation (non-migration) error, it is possible for the lock to not be released. Until
		// a better locking mechanism is added, a manual purging of the lock table may be required in such circumstances
		query := "DELETE FROM " + c.config.LockTable + " WHERE lock_id = $1"
		if _, err := c.db.Exec(query, aid); err != nil {
			if e, ok := err.(*pq.Error); ok {
				// 42P01 is "UndefinedTableError" in YugabyteDB
				// https://github.com/yugabyte/yugabyte-db/blob/9c6b8e6beb56eed8eeb357178c0c6b837eb49896/src/postgres/src/backend/utils/errcodes.txt#L366
				if e.Code == "42P01" {
					// On drops, the lock table is fully removed; This is fine, and is a valid "unlocked" state for the schema
					return nil
				}
			}

			return database.Error{OrigErr: err, Err: "failed to release migration lock", Query: []byte(query)}
		}

		return nil
	})
}

func (c *YugabyteDB) Run(migration io.Reader) error {
	migr, err := io.ReadAll(migration)
	if err != nil {
		return err
	}

	// run migration
	query := string(migr[:])
	if _, err := c.db.Exec(query); err != nil {
		return database.Error{OrigErr: err, Err: "migration failed", Query: migr}
	}

	return nil
}

func (c *YugabyteDB) SetVersion(version int, dirty bool) error {
	return c.doTxWithRetry(context.Background(), &sql.TxOptions{Isolation: sql.LevelSerializable}, func(tx *sql.Tx) error {
		if _, err := tx.Exec(`DELETE FROM "` + c.config.MigrationsTable + `"`); err != nil {
			return err
		}

		// Also re-write the schema version for nil dirty versions to prevent
		// empty schema version for failed down migration on the first migration
		// See: https://github.com/golang-migrate/migrate/issues/330
		if version >= 0 || (version == database.NilVersion && dirty) {
			if _, err := tx.Exec(`INSERT INTO "`+c.config.MigrationsTable+`" (version, dirty) VALUES ($1, $2)`, version, dirty); err != nil {
				return err
			}
		}

		return nil
	})
}

func (c *YugabyteDB) Version() (version int, dirty bool, err error) {
	query := `SELECT version, dirty FROM "` + c.config.MigrationsTable + `" LIMIT 1`
	err = c.db.QueryRow(query).Scan(&version, &dirty)

	switch {
	case err == sql.ErrNoRows:
		return database.NilVersion, false, nil

	case err != nil:
		if e, ok := err.(*pq.Error); ok {
			// 42P01 is "UndefinedTableError" in YugabyteDB
			// https://github.com/yugabyte/yugabyte-db/blob/9c6b8e6beb56eed8eeb357178c0c6b837eb49896/src/postgres/src/backend/utils/errcodes.txt#L366
			if e.Code == "42P01" {
				return database.NilVersion, false, nil
			}
		}
		return 0, false, &database.Error{OrigErr: err, Query: []byte(query)}

	default:
		return version, dirty, nil
	}
}

func (c *YugabyteDB) Drop() (err error) {
	query := `SELECT table_name FROM information_schema.tables WHERE table_schema=(SELECT current_schema()) AND table_type='BASE TABLE'`
	tables, err := c.db.Query(query)
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
		for _, t := range tableNames {
			query = `DROP TABLE IF EXISTS ` + t + ` CASCADE`
			if _, err := c.db.Exec(query); err != nil {
				return &database.Error{OrigErr: err, Query: []byte(query)}
			}
		}
	}

	return nil
}

// ensureVersionTable checks if versions table exists and, if not, creates it.
// Note that this function locks the database
func (c *YugabyteDB) ensureVersionTable() (err error) {
	if err = c.Lock(); err != nil {
		return err
	}

	defer func() {
		if e := c.Unlock(); e != nil {
			if err == nil {
				err = e
			} else {
				err = multierror.Append(err, e)
			}
		}
	}()

	// check if migration table exists
	var count int
	query := `SELECT COUNT(1) FROM information_schema.tables WHERE table_name = $1 AND table_schema = (SELECT current_schema()) LIMIT 1`
	if err := c.db.QueryRow(query, c.config.MigrationsTable).Scan(&count); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}
	if count == 1 {
		return nil
	}

	// if not, create the empty migration table
	query = `CREATE TABLE "` + c.config.MigrationsTable + `" (version INT NOT NULL PRIMARY KEY, dirty BOOL NOT NULL)`
	if _, err := c.db.Exec(query); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}
	return nil
}

func (c *YugabyteDB) ensureLockTable() error {
	// check if lock table exists
	var count int
	query := `SELECT COUNT(1) FROM information_schema.tables WHERE table_name = $1 AND table_schema = (SELECT current_schema()) LIMIT 1`
	if err := c.db.QueryRow(query, c.config.LockTable).Scan(&count); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}
	if count == 1 {
		return nil
	}

	// if not, create the empty lock table
	query = `CREATE TABLE "` + c.config.LockTable + `" (lock_id TEXT NOT NULL PRIMARY KEY)`
	if _, err := c.db.Exec(query); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}

	return nil
}

func (c *YugabyteDB) doTxWithRetry(
	ctx context.Context,
	txOpts *sql.TxOptions,
	fn func(tx *sql.Tx) error,
) error {
	backOff := c.newBackoff(ctx)

	return backoff.Retry(func() error {
		tx, err := c.db.BeginTx(ctx, txOpts)
		if err != nil {
			return backoff.Permanent(err)
		}

		// If we've tried to commit the transaction Rollback just returns sql.ErrTxDone.
		//nolint:errcheck
		defer tx.Rollback()

		if err := fn(tx); err != nil {
			if errIsRetryable(err) {
				return err
			}

			return backoff.Permanent(err)
		}

		if err := tx.Commit(); err != nil {
			if errIsRetryable(err) {
				return err
			}

			return backoff.Permanent(err)
		}

		return nil
	}, backOff)
}

func (c *YugabyteDB) newBackoff(ctx context.Context) backoff.BackOff {
	if ctx == nil {
		ctx = context.Background()
	}

	retrier := backoff.WithMaxRetries(backoff.WithContext(&backoff.ExponentialBackOff{
		InitialInterval:     backoff.DefaultInitialInterval,
		RandomizationFactor: backoff.DefaultRandomizationFactor,
		Multiplier:          backoff.DefaultMultiplier,
		MaxInterval:         c.config.MaxRetryInterval,
		MaxElapsedTime:      c.config.MaxRetryElapsedTime,
		Stop:                backoff.Stop,
		Clock:               backoff.SystemClock,
	}, ctx), uint64(c.config.MaxRetries))

	retrier.Reset()

	return retrier
}

func errIsRetryable(err error) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}

	// Assume that it's safe to retry 08006 and XX000 because we check for lock existence
	// before creating and lock ID is primary key. Version field in migrations table is primary key too
	// and delete all versions is an idempotent operation.
	return pgErr.Code == pgerrcode.SerializationFailure || // optimistic locking conflict
		pgErr.Code == pgerrcode.DeadlockDetected ||
		pgErr.Code == pgerrcode.ConnectionFailure || // node down, need to reconnect
		pgErr.Code == pgerrcode.InternalError // may happen during HA
}
