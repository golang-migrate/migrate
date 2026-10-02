// Package dsql implements the database.Driver interface for Amazon Aurora DSQL.
//
// Aurora DSQL speaks PostgreSQL's wire protocol, and two of its behaviors put the existing
// postgres and pgx drivers out of reach:
//
//   - pg_advisory_lock and pg_advisory_unlock answer SQLSTATE 0A000, so the migration lock is a
//     row in a table.
//   - TRUNCATE answers 0A000, so SetVersion clears the version table with a WHERE-less DELETE.
//
// DSQL also holds a transaction to one DDL statement and keeps DDL apart from DML. Neither
// postgres nor pgx wraps a migration file in an explicit transaction either, so that shapes how
// migrations are written rather than what this driver does. See the README.
//
// This driver's own two tables are schema-qualified from x-migrations-schema or CURRENT_SCHEMA(),
// WithInstance included, whose caller supplies a pool this driver never configures. Open also
// honors search_path from the URL as the postgres and pgx drivers do, through a pool hook, so
// CURRENT_SCHEMA() resolves from it and x-migrations-schema stays an override; see poolConfigFor.
//
// IAM authentication and optimistic-concurrency retry come from the AWS connector
// (github.com/awslabs/aurora-dsql-connectors/go/pgx) rather than being reimplemented here. Open
// builds its pool, so a DSQL URL carries a user and no password and a token is generated per
// connection. WithInstance resolves no credentials and opens no connections.
//
// Unlike the postgres drivers this one pins no single *sql.Conn. Those pin one because an advisory
// lock is session-scoped; a lock held as a table row lets database/sql hand out any connection,
// which is what lets a run outlive the 60 minutes DSQL allows a connection and leaves the
// connector free to recycle at 55.
//
// A migration that builds an index is asynchronous: CREATE INDEX ASYNC hands back a job_id once
// the build is enqueued, and Run waits for it, so the version migrate marks clean describes a
// schema that is in place. See applyStatement.
package dsql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	nurl "net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	awsdsql "github.com/awslabs/aurora-dsql-connectors/go/pgx/dsql"
	"github.com/awslabs/aurora-dsql-connectors/go/pgx/occretry"
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
	"github.com/golang-migrate/migrate/v4/database/multistmt"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
)

var (
	DefaultMigrationsTable       = "schema_migrations"
	DefaultLockTable             = "schema_migrations_lock"
	DefaultMultiStatementMaxSize = 10 * 1 << 20 // 10 MB

	multiStmtDelimiter = []byte(";")
)

// OCC retry defaults.
//
// DefaultOCCMaxRetries is 0, so retry is opt-in: this driver imposes no retry count of its
// own and runs a statement once unless asked for more. Both entry points agree on that,
// which a non-zero default cannot manage, since a Config literal's zero is
// indistinguishable from "unset". Setting it is still worth it: DSQL raises OC001
// non-deterministically even without contention, from a stale catalog cache after a DDL,
// and WithInstance issues two DDLs before migrate's first read.
//
// DefaultOCCMaxRetryDelay does apply when unset, because 0 is not a value the connector can
// use: see occConfig.
const (
	DefaultOCCMaxRetries    = 0
	DefaultOCCMaxRetryDelay = 5 * time.Second

	// minOCCRetryDelay floors the delay handed to the connector. occretry's backoff
	// computes jitter as rand.Int63n(int64(wait/4)), which panics once wait drops below
	// 4ns, and a Config literal takes a time.Duration directly — OCCMaxRetryDelay: 3 is
	// three nanoseconds, not three seconds. One millisecond is also the finest value
	// x-occ-max-retry-delay can express.
	minOCCRetryDelay = time.Millisecond

	// Retry floors on the driver's own statements: retryAtLeast raises OCCMaxRetries to the floor
	// and keeps a larger setting. Each answers to a different constraint, so each has its own name.
	//
	// lockRetries is one, which is what a 40001 loser needs to see the winner's row and report
	// ErrLocked. One retry costs at most InitialWait plus jitter, keeping the acquire path inside
	// migrate's 15-second LockTimeout however OCCMaxRetryDelay is set, since occConfig clamps
	// InitialWait down to it. Lock holds it as a fixed count rather than a floor — see retryAcquire.
	//
	// unlockRetries is ten. m.unlock() calls the driver synchronously, so no deadline races the
	// release path and a late success still lands, while giving up early leaves a row every later
	// run needs x-force-lock to clear.
	//
	// bootstrapRetries is one, for the two CREATE TABLE IF NOT EXISTS statements newDriver issues.
	// See ensureLockTable.
	lockRetries      = 1
	unlockRetries    = 10
	bootstrapRetries = 1
)

var (
	ErrNilConfig      = errors.New("no config")
	ErrNoDatabaseName = errors.New("no database name")
	ErrNoSchema       = errors.New("no schema")

	// ErrPasswordSet rejects a password in a dsql:// URL. The connector authenticates with an IAM
	// token per connection and discards any password, so accepting one would ignore it silently.
	ErrPasswordSet = errors.New("dsql: password not supported, authentication uses IAM")
)

func init() {
	database.Register("dsql", &DSQL{})
}

type Config struct {
	MigrationsTable       string
	LockTable             string
	ForceLock             bool
	DatabaseName          string
	SchemaName            string
	StatementTimeout      time.Duration
	MultiStatementEnabled bool
	MultiStatementMaxSize int

	// OCC conflict retry. Classification, backoff and the meaning of these values are the
	// connector's; occConfig hands them over unchanged.
	//
	// OCCMaxRetries counts retries, not attempts, so occretry.Config's zero applies here too:
	// 0 runs a statement exactly once, and 0 is the default — see DefaultOCCMaxRetries for why
	// opting in is worth it. Negative values are a programming error: occretry's loop body
	// never runs, so the statement is skipped and the caller is told retries were exhausted.
	// Both entry points hold the field to zero or more — parseConfig on the URL option,
	// validate on a literal.
	//
	// OCCMaxRetryDelay is a Duration, so an unsuffixed literal is nanoseconds: write
	// 3 * time.Second, not 3. Unlike the retry count, 0 here cannot be passed through: it
	// would leave the connector no room to back off and panic its jitter, so 0 means
	// "unset" and takes the connector's 5s. See occConfig.
	OCCMaxRetries    int
	OCCMaxRetryDelay time.Duration

	// An unexported field forces composite literals outside this package to be keyed, so a
	// field can be added later without breaking callers.
	_ struct{}
}

type DSQL struct {
	db       *sql.DB
	isLocked atomic.Bool

	// lockTableDropped records that Drop removed the lock table, so the Unlock Migrate issues
	// next has no row of this instance's to delete. Lock clears it; see Unlock.
	lockTableDropped atomic.Bool

	// pool is set only by Open, which builds it. stdlib.OpenDBFromPool does not take ownership,
	// so closing db alone would leave the pool and its connections behind; Close closes both. A
	// WithInstance caller owns their own pool and leaves this nil.
	pool *pgxpool.Pool

	config *Config
}

// WithInstance returns a driver for an existing *sql.DB. It neither resolves AWS
// credentials nor opens connections; a caller wanting automatic IAM auth builds the pool
// with awsdsql.NewPool and wraps it via stdlib.OpenDBFromPool.
func WithInstance(instance *sql.DB, config *Config) (database.Driver, error) {
	// Assigned and returned separately so a failure hands back a nil interface, which is what
	// a caller testing driver != nil reads.
	d, err := newDriver(instance, config)
	if err != nil {
		return nil, err
	}
	return d, nil
}

// newDriver is WithInstance returning the concrete type, so Open can attach the pool it built to
// the driver it gets back. WithInstance returns database.Driver to match the other drivers, and a
// type assertion on the way back out would break silently if that signature ever changed.
func newDriver(instance *sql.DB, config *Config) (*DSQL, error) {
	if config == nil {
		return nil, ErrNilConfig
	}
	if err := config.validate(); err != nil {
		return nil, err
	}

	config.setDefaults()

	// After setDefaults, because a default supplies half of the collision it rejects.
	if err := config.validateTableNames(); err != nil {
		return nil, err
	}

	if err := instance.Ping(); err != nil {
		return nil, err
	}

	d := &DSQL{db: instance, config: config}

	// CURRENT_DATABASE() and CURRENT_SCHEMA() fill what the caller left unset, as they do in
	// the postgres and pgx drivers. On the dsql:// path a search_path in the URL has already
	// reached the connection by now, so CURRENT_SCHEMA() resolves from it and
	// x-migrations-schema stays an override.
	if config.DatabaseName == "" {
		query := `SELECT CURRENT_DATABASE()`
		var databaseName string
		if err := instance.QueryRow(query).Scan(&databaseName); err != nil {
			return nil, &database.Error{OrigErr: err, Query: []byte(query)}
		}
		if databaseName == "" {
			return nil, ErrNoDatabaseName
		}
		config.DatabaseName = databaseName
	}

	if config.SchemaName == "" {
		query := `SELECT CURRENT_SCHEMA()`
		// NullString so an unresolved search_path reports ErrNoSchema: CURRENT_SCHEMA() answers
		// NULL there, which is what database/postgres scans for since #696.
		var schemaName sql.NullString
		if err := instance.QueryRow(query).Scan(&schemaName); err != nil {
			return nil, &database.Error{OrigErr: err, Query: []byte(query)}
		}
		if !schemaName.Valid || schemaName.String == "" {
			return nil, ErrNoSchema
		}
		config.SchemaName = schemaName.String
	}

	// The lock table comes first because ensureVersionTable takes the lock, a row in it.
	if err := d.ensureLockTable(); err != nil {
		return nil, err
	}
	if err := d.ensureVersionTable(); err != nil {
		return nil, err
	}

	return d, nil
}

// validate holds a Config to the range occretry accepts. It covers WithInstance, where the
// Config arrives as a literal rather than as URL options.
//
// OCCMaxRetries is the field that needs it: occretry's loop runs while attempt <= MaxRetries,
// so a negative skips the statement and reports retries exhausted. setDefaults clamps the
// rest, and the remaining fields report at first use.
func (c *Config) validate() error {
	if c.OCCMaxRetries < 0 {
		return fmt.Errorf("OCCMaxRetries must not be negative, got %d", c.OCCMaxRetries)
	}
	return nil
}

// validateTableNames rejects a configuration whose two tables are one table. It runs after
// setDefaults, because a default supplies half of the collision: x-migrations-table set to
// schema_migrations_lock, with no x-lock-table, leaves both names equal to DefaultLockTable.
//
// One table cannot serve both — the lock path expects a lock_id column, the version path expects
// version and dirty — and nothing downstream catches it. ensureLockTable creates the table,
// ensureVersionTable's CREATE TABLE IF NOT EXISTS is then a no-op, newDriver hands back a driver
// that looks healthy, and migrate's first Version() reports an undefined column rather than the
// misconfiguration that caused it.
//
// Comparing the names alone is enough: qualifiedTable places both in the one schema, so there is
// no configuration in which equal names are different tables.
func (c *Config) validateTableNames() error {
	if c.MigrationsTable == c.LockTable {
		return fmt.Errorf("dsql: the migrations table and the lock table are both %q, and one table cannot serve both; "+
			"set x-migrations-table or x-lock-table (MigrationsTable or LockTable) so they differ", c.MigrationsTable)
	}
	return nil
}

// setDefaults fills in the optional fields. Required fields are left alone so a missing
// one surfaces as an error rather than a guess.
func (c *Config) setDefaults() {
	if c.MigrationsTable == "" {
		c.MigrationsTable = DefaultMigrationsTable
	}
	if c.LockTable == "" {
		c.LockTable = DefaultLockTable
	}
	if c.MultiStatementMaxSize <= 0 {
		c.MultiStatementMaxSize = DefaultMultiStatementMaxSize
	}
	// OCCMaxRetries is deliberately not defaulted here. Zero is the connector's "no retry"
	// and defaulting it away would make that value unreachable.
	if c.OCCMaxRetryDelay <= 0 {
		c.OCCMaxRetryDelay = DefaultOCCMaxRetryDelay
	}
}

// occConfig maps the driver's OCC settings onto the connector's retry config. It starts
// from occretry.DefaultConfig, which is how the connector expects to be configured: its
// Config has no normalization of its own, so a zero value would leave Multiplier and
// MaxWait at zero as well.
//
// MaxRetries is handed over unchanged, so the connector's semantics reach the caller
// intact rather than being reinterpreted here.
//
// MaxWait cannot be, and the exception is the connector's doing: it clamps every subsequent
// wait to MaxWait, so a MaxWait under the jitter floor panics on the second retry — see
// minOCCRetryDelay. InitialWait is lowered to match, because the connector seeds the first wait
// from it and only clamps later ones, so a MaxWait below InitialWait would otherwise be ignored
// on the first retry.
func (c *Config) occConfig() occretry.Config {
	cfg := occretry.DefaultConfig()

	cfg.MaxRetries = c.OCCMaxRetries

	cfg.MaxWait = max(c.OCCMaxRetryDelay, minOCCRetryDelay)
	cfg.InitialWait = min(cfg.InitialWait, cfg.MaxWait)

	return cfg
}

// Open connects to Aurora DSQL. The URL carries no password: the connector generates an
// IAM token per connection.
//
//	dsql://admin@cluster.dsql.us-east-1.on.aws:5432/postgres?x-migrations-table=…
//
// The connector resolves the region from ?region=, then the hostname, then the
// environment, and enforces TLS unconditionally — an sslmode in the URL is ignored.
func (d *DSQL) Open(rawURL string) (database.Driver, error) {
	purl, err := nurl.Parse(rawURL)
	if err != nil {
		return nil, err
	}

	config, err := parseConfig(purl)
	if err != nil {
		return nil, err
	}

	poolConfig, err := poolConfigFor(purl.Query().Get("search_path"))
	if err != nil {
		return nil, err
	}

	// FilterCustomQuery strips only x-*, so region, profile and tokenDurationSecs
	// survive for the connector to parse itself.
	pool, err := awsdsql.NewPool(context.Background(), migrate.FilterCustomQuery(purl).String(), poolConfig)
	if err != nil {
		return nil, err
	}

	db := stdlib.OpenDBFromPool(pool)
	driver, err := newDriver(db, config)
	if err != nil {
		closeErr := db.Close()
		pool.Close()
		return nil, errors.Join(err, closeErr)
	}
	driver.pool = pool
	return driver, nil
}

// poolDSN is the connection string poolConfigFor parses, so the dsql:// URL configures the pool
// rather than the PG* environment.
//
// pgconn.ParseConfig merges its defaults, then the environment, then the connection string, so the
// empty string leaves every PG* variable in the config with nothing overriding it — PGPORT alone
// fails Open, rejected during parsing before the URL's valid port is read. Host, port, user and
// database here are placeholders the connector overwrites from the URL; sslmode=disable clears the
// primary TLSConfig alone, which it replaces with its own TLS 1.2 config, so TLS stays
// unconditional.
//
// The rest pin what configureConnConfig leaves alone — it replaces Host, Port, Database, User,
// TLSConfig, RuntimeParams and the query exec mode, and TestPoolConfigSuppressesEveryPGVariable
// holds each remaining key to its pin. Three are worth knowing before editing the string. Empty
// values are quoted, so pgx's keyword/value parser reads them as values rather than taking the next
// key as one. sslnegotiation keeps the SSLRequest path, which is the one DSQL completes: it closes a
// direct TLS handshake and pgx reports "unexpected EOF", naming neither TLS nor PGSSLNEGOTIATION.
// And application_name, timezone and options are left out, each being a RuntimeParams key where an
// empty value reaches the startup packet and DSQL answers 08P01 — the connector replacing that map
// is what suppresses those three. See poolConfigFor.
const poolDSN = "host=localhost port=5432 user=postgres database=postgres" +
	" connect_timeout=0 sslmode=disable target_session_attrs=any" +
	" sslrootcert='' sslcert='' sslkey=''" +
	" sslnegotiation=postgres min_protocol_version=3.0 max_protocol_version=3.0" +
	" password='' passfile=''"

// poolConfigFor builds the pool configuration Open hands the connector.
//
// It exists for search_path. pgx keeps a search_path from the URL in ConnConfig.RuntimeParams and
// sends it in the startup packet, and the connector assigns that field a fresh map holding only its
// own application_name while building the pool, which makes a hook the route the value takes.
// BeforeConnect lands after the connector is finished: pgxpool hands the hook a per-connection copy
// of the ConnConfig and the connector chains the hook ahead of its own, so every connection a
// migration can be handed starts with the schema set.
//
// The value goes over verbatim, so the server parses it exactly as it would through postgres or
// pgx, down to case folding, quoting and which values it declines; parsing it here would answer each
// of those differently. TestSearchPathHookPreservesRawValue holds it to that.
//
// Only search_path is carried over. rejectIgnoredOptions refuses the options spelling of it, and
// the connector drops any other PostgreSQL parameter in the URL.
//
// Passing a pool config at all costs the connector's lifetime defaults, so both are pinned back to
// its values. It fills MaxConnLifetime and MaxConnIdleTime only when they are zero, and
// pgxpool.ParseConfig has already set 1h and 30m — an hour being exactly when DSQL closes a
// connection server-side, which would leave the pool handing out connections the server is about to
// drop.
//
// poolDSN keeps every PG* variable out of the config but PGSERVICE, and the connector's replacement
// of RuntimeParams accounts for PGAPPNAME, PGTZ and PGOPTIONS. PGSERVICE is resolved before
// connection-string settings are merged, so no service= of our own can outrank it, and naming the
// key empty is worse than leaving it out: an empty value still counts as present and sends every
// Open to ~/.pg_service.conf. So an unreadable service file fails Open loudly, which the connector
// does too when it builds a pool config itself, rather than misconfiguring quietly. Bypassing
// ParseConfig is no answer either — pgxpool.NewWithConfig panics on a Config it did not build.
func poolConfigFor(searchPath string) (*pgxpool.Config, error) {
	poolConfig, err := pgxpool.ParseConfig(poolDSN)
	if err != nil {
		return nil, err
	}

	// Set by poolDSN already, and pinned again so that pgx mapping a further PG* variable onto one
	// of these fields cannot reach the pool.
	poolConfig.ConnConfig.ConnectTimeout = 0
	poolConfig.ConnConfig.Fallbacks = nil
	poolConfig.ConnConfig.ValidateConnect = nil

	poolConfig.MaxConnLifetime = awsdsql.DefaultMaxConnLifetime
	poolConfig.MaxConnIdleTime = awsdsql.DefaultMaxConnIdleTime

	// A search_path in the URL gets the hook; without one the connector's own startup parameters
	// stand. RuntimeParams is always there to write to: ParseConfig above creates the map, and the
	// connector's replacement is a map literal.
	if searchPath != "" {
		poolConfig.BeforeConnect = func(_ context.Context, connConfig *pgx.ConnConfig) error {
			connConfig.RuntimeParams["search_path"] = searchPath
			return nil
		}
	}

	return poolConfig, nil
}

// parseConfig reads the driver's options from the URL. Every option this function returns comes
// from the URL, or from the Config passed to WithInstance, and never from the environment.
//
// Two things below this do read it, neither of them driver configuration. The connector resolves
// AWS credentials and region from the environment, where an explicit ?region= always wins. And
// pgx reads PG* variables while parsing a pool config, which poolDSN is there to override — see
// poolConfigFor for the one that survives it.
func parseConfig(purl *nurl.URL) (*Config, error) {
	query := purl.Query()

	// Rejected rather than ignored, for the reason ErrPasswordSet gives. An empty password still
	// counts: "admin:@host" names the field, and treating that as absent restores the ambiguity.
	if purl.User != nil {
		if _, set := purl.User.Password(); set {
			return nil, ErrPasswordSet
		}
	}
	// The query spelling needs its own check: FilterCustomQuery strips only x-*, so password=
	// travels to the connector, which reads region, profile and tokenDurationSecs and leaves every
	// other key where it lies. query.Has reports the key, so "?password=" counts too.
	if query.Has("password") {
		return nil, ErrPasswordSet
	}

	if err := rejectIgnoredOptions(query); err != nil {
		return nil, err
	}

	statementTimeout, err := parseInt(query, "x-statement-timeout", 0)
	if err != nil {
		return nil, err
	}

	multiStatementMaxSize, err := parseInt(query, "x-multi-statement-max-size", DefaultMultiStatementMaxSize)
	if err != nil {
		return nil, err
	}
	if multiStatementMaxSize <= 0 {
		multiStatementMaxSize = DefaultMultiStatementMaxSize
	}

	occMaxRetries, err := parseInt(query, "x-occ-max-retries", DefaultOCCMaxRetries)
	if err != nil {
		return nil, err
	}
	// Rejected for the reason validate gives. Zero needs no handling: it is both the default and
	// the connector's "run once", so a URL and a Config literal resolve it the same way.
	if occMaxRetries < 0 {
		return nil, fmt.Errorf("x-occ-max-retries must not be negative, got %d", occMaxRetries)
	}

	occMaxRetryDelay, err := parseInt(query, "x-occ-max-retry-delay", int(DefaultOCCMaxRetryDelay/time.Millisecond))
	if err != nil {
		return nil, err
	}
	if occMaxRetryDelay < 0 {
		return nil, fmt.Errorf("x-occ-max-retry-delay must not be negative, got %d", occMaxRetryDelay)
	}

	forceLock, err := parseBool(query, "x-force-lock")
	if err != nil {
		return nil, err
	}

	multiStatementEnabled, err := parseBool(query, "x-multi-statement")
	if err != nil {
		return nil, err
	}

	return &Config{
		DatabaseName:          strings.TrimPrefix(purl.Path, "/"),
		SchemaName:            query.Get("x-migrations-schema"),
		MigrationsTable:       query.Get("x-migrations-table"),
		LockTable:             query.Get("x-lock-table"),
		ForceLock:             forceLock,
		StatementTimeout:      time.Duration(statementTimeout) * time.Millisecond,
		MultiStatementEnabled: multiStatementEnabled,
		MultiStatementMaxSize: multiStatementMaxSize,
		OCCMaxRetries:         occMaxRetries,
		OCCMaxRetryDelay:      time.Duration(occMaxRetryDelay) * time.Millisecond,
	}, nil
}

// rejectIgnoredOptions fails on options this driver cannot honor, rather than dropping
// them. Same reasoning as ErrPasswordSet: an operator who sets one of these has an
// expectation, and silently discarding it is worse than refusing to start.
func rejectIgnoredOptions(query nurl.Values) error {
	// search_path is honored by Open, via poolConfigFor. The options spelling of it carries
	// arbitrary -c flags alongside, which this driver would have to parse and apply one at a time,
	// so it is refused whole and the error names the spelling that is applied. The options form is
	// what the Aurora DSQL ORM guides tell users to write, which is why it is worth naming.
	if query.Has("options") {
		return errors.New("options is not supported: it carries arbitrary -c flags, and this driver applies search_path alone. " +
			"Write search_path=app instead of options=-c search_path=app, which Open applies to every connection")
	}
	// postgres can place the migrations table in a schema other than the working one, which
	// this driver cannot express: x-migrations-schema moves both of its tables together.
	if query.Has("x-migrations-table-quoted") {
		return errors.New("x-migrations-table-quoted is not supported, identifiers are always quoted")
	}
	return nil
}

func parseInt(query nurl.Values, key string, fallback int) (int, error) {
	value := query.Get(key)
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("unable to parse %s: %w", key, err)
	}
	return parsed, nil
}

func parseBool(query nurl.Values, key string) (bool, error) {
	value := query.Get(key)
	if value == "" {
		return false, nil
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return false, fmt.Errorf("unable to parse %s: %w", key, err)
	}
	return parsed, nil
}

// retry runs fn, re-running it on a DSQL optimistic-concurrency conflict. Classification
// (OC000, OC001, 40001) and the backoff schedule are the connector's; this is the single
// place the driver applies them.
//
// occretry.IsOCCError matches with errors.As on *pgconn.PgError, which pgx's stdlib
// wrapper preserves through database/sql, so the pgx-native helpers are not needed.
//
// fn must return the error it received, unwrapped. database.Error has no Unwrap method, so
// wrapping inside fn hides the *pgconn.PgError: IsOCCError returns false and retry quietly
// degrades to a single attempt. Nothing reports this — the wrapped error still describes a
// real failure, so the lost retry is invisible. The same holds for database.ErrLocked and
// every other sentinel. Add migrate's error context to what this function returns instead;
// occretry wraps its own exhaustion error with %w, so the PgError survives.
func (d *DSQL) retry(ctx context.Context, fn func() error) error {
	return occretry.Retry(ctx, d.config.occConfig(), fn)
}

// retryAtLeast runs fn like retry, with a floor under the retry count, and keeps a larger setting.
// It covers Unlock and the bootstrap statements, where a second attempt decides whether the answer
// is right rather than merely faster and OCCMaxRetries defaults to 0. Each floor's reasoning is at
// lockRetries, which is also why the floor is a parameter rather than one constant.
//
// The backoff schedule stays the caller's: MaxWait comes from OCCMaxRetryDelay, so the ceiling here
// moves with x-occ-max-retry-delay.
func (d *DSQL) retryAtLeast(ctx context.Context, retries int, fn func() error) error {
	cfg := d.config.occConfig()
	cfg.MaxRetries = max(cfg.MaxRetries, retries)
	return occretry.Retry(ctx, cfg, fn)
}

// retryAcquire runs fn like retry, with the count held at lockRetries rather than floored by it.
// Lock is the only caller, because acquisition is the one path with a deadline racing it that
// nothing cancels: migrate runs the driver's Lock in a goroutine against LockTimeout (migrate.go)
// and abandons it on timeout while it keeps going, so a budget wider than that timeout lets a late
// success commit a lock row after the command has already exited on ErrLockTimeout — a row every
// later run needs x-force-lock to clear.
//
// So x-occ-max-retries does not reach acquisition. It still applies to migration statements,
// SetVersion, Version, Drop, and to Unlock above the unlockRetries floor.
func (d *DSQL) retryAcquire(ctx context.Context, fn func() error) error {
	cfg := d.config.occConfig()
	cfg.MaxRetries = lockRetries
	return occretry.Retry(ctx, cfg, fn)
}

// Close releases the *sql.DB, and the pool behind it when Open built one. Closing the
// *sql.DB does not close that pool: stdlib.OpenDBFromPool wraps it without taking
// ownership, so a driver Open created would otherwise leave its connections open.
func (d *DSQL) Close() error {
	var err error
	if d.db != nil {
		err = d.db.Close()
	}
	if d.pool != nil {
		d.pool.Close()
	}
	return err
}

// Lock takes the migration lock by inserting a row, because Aurora DSQL has no advisory
// locks. The row's primary key is the arbiter, but unlike PostgreSQL the loser is not
// always a unique violation: DSQL adjudicates conflicts at COMMIT under snapshot
// isolation, so two migrators inserting the same key concurrently both succeed at
// statement time and the loser commits with SQLSTATE 40001. A migrator that starts after the
// winner has committed sees the row in its snapshot instead, which a bare INSERT would report
// as 23505.
//
// So "already held" is treated as a value rather than an error: INSERT ... ON CONFLICT DO
// NOTHING, and no rows affected means someone else holds it. That one arm answers for both
// losers — the late arrival on its first attempt, the concurrent one on the retry lockRetries
// provides. cockroachdb takes the same line, wrapping its whole Lock in crdb.ExecuteTx.
//
// A process killed while holding the lock leaves the row behind; x-force-lock lets an
// operator delete it deliberately. Only here, on a lock they asked to take: the acquire Open makes
// to create the version table goes through lock(false). See ensureVersionTable.
func (d *DSQL) Lock() error {
	return d.lock(d.config.ForceLock)
}

// lock takes the lock, releasing whatever row is there first when force is set.
func (d *DSQL) lock(force bool) error {
	return database.CasRestoreOnErr(&d.isLocked, false, true, database.ErrLocked, func() error {
		ctx := context.Background()

		lockID, err := d.lockID()
		if err != nil {
			return err
		}

		// Its own retried unit, separate from the INSERT below, so a retry of the INSERT runs the
		// INSERT alone. Force-lock releases whichever row is there, stale or live, so it is the
		// operator who decides no other migration is running — hence opt-in.
		if force {
			query := `DELETE FROM ` + d.qualifiedTable(d.config.LockTable) + ` WHERE lock_id = $1`
			if err := d.retryAcquire(ctx, func() error {
				_, err := d.db.ExecContext(ctx, query, lockID)
				return err
			}); err != nil {
				return database.Error{OrigErr: err, Err: "failed to force the migration lock", Query: []byte(query)}
			}
		}

		query := `INSERT INTO ` + d.qualifiedTable(d.config.LockTable) + ` (lock_id) VALUES ($1) ON CONFLICT (lock_id) DO NOTHING`
		err = d.retryAcquire(ctx, func() error {
			result, err := d.db.ExecContext(ctx, query, lockID)
			if err != nil {
				return err
			}
			affected, err := result.RowsAffected()
			if err != nil {
				return err
			}
			if affected == 0 {
				// Bare, so errors.Is matches it; a database.Error would hide it — see retry.
				return database.ErrLocked
			}
			return nil
		})
		if errors.Is(err, database.ErrLocked) {
			return database.ErrLocked
		}
		if err != nil {
			return database.Error{OrigErr: err, Err: "failed to take the migration lock", Query: []byte(query)}
		}
		// This instance holds a row of its own again, so Unlock has something to release. Clears
		// the suppression a preceding Drop set; see Unlock.
		d.lockTableDropped.Store(false)
		return nil
	})
}

func (d *DSQL) Unlock() error {
	return database.CasRestoreOnErr(&d.isLocked, true, false, database.ErrNotLocked, func() error {
		ctx := context.Background()

		// Drop took the lock table with it, so this instance has no row left to release, and the
		// DELETE below would reach a table another process has since recreated. Its row would be the
		// one deleted: lockID comes from configuration alone, so every migrator sharing that
		// configuration computes the same id and nothing in the row records who wrote it. Migrate
		// calls Unlock straight after Drop (migrate.go), which is the window. Skipping the DELETE
		// leaves that process holding the lock it took, and isLocked returns to false either way.
		if d.lockTableDropped.Load() {
			return nil
		}

		lockID, err := d.lockID()
		if err != nil {
			return err
		}

		// unlockRetries rather than OCCMaxRetries: the row is durable, so a DELETE that gives up
		// leaves a lock nobody holds. See lockRetries.
		query := `DELETE FROM ` + d.qualifiedTable(d.config.LockTable) + ` WHERE lock_id = $1`
		err = d.retryAtLeast(ctx, unlockRetries, func() error {
			_, err := d.db.ExecContext(ctx, query, lockID)
			return err
		})
		// Drop removes the lock table and Migrate still calls Unlock afterwards, so a missing
		// table is a released lock rather than a failure.
		if err != nil && !isUndefinedTable(err) {
			return database.Error{OrigErr: err, Err: "failed to release the migration lock", Query: []byte(query)}
		}
		return nil
	})
}

// lockID derives the lock row's key from everything that identifies the lock, including
// the lock table itself: two migrators pointed at the same schema and migrations table but
// different x-lock-table values would otherwise compute the same key and store it in
// different tables, excluding nobody. PostgreSQL cannot express that, since its advisory
// lock has no table to vary.
func (d *DSQL) lockID() (string, error) {
	return database.GenerateAdvisoryLockId(
		d.config.DatabaseName,
		d.config.SchemaName,
		d.config.MigrationsTable,
		d.config.LockTable,
	)
}

// Run applies a migration.
//
// By default the whole file is sent as one Exec. That relies on pgx dropping to the simple
// protocol, which it does only when the Exec carries no bound arguments; the server then
// treats the whole string as one implicit transaction. So a multi-statement file fails
// atomically and nothing is half-applied, and an OCC conflict committed nothing, which is
// why this path is safe to retry. Passing even one parameter would send the file through
// the extended protocol, where it is a syntax error.
//
// x-multi-statement splits the file at semicolons so each DDL gets its own transaction,
// which DSQL requires for a file holding more than one DDL. Each piece is retried on its
// own; what must never be replayed is a piece that already committed.
func (d *DSQL) Run(migration io.Reader) error {
	ctx := context.Background()

	if d.config.MultiStatementEnabled {
		// Nothing wraps this loop; each piece carries its own retry, inside applyStatement.
		//
		// The splitter is a plain byte search for ";" with no SQL awareness, so it breaks
		// dollar-quoted bodies, string literals and comments. Hence off by default.
		var runErr error
		if err := multistmt.Parse(migration, multiStmtDelimiter, d.config.MultiStatementMaxSize, func(statement []byte) bool {
			runErr = d.applyStatement(ctx, statement)
			return runErr == nil
		}); err != nil {
			return err
		}
		return runErr
	}

	statement, err := io.ReadAll(migration)
	if err != nil {
		return err
	}
	return d.applyStatement(ctx, statement)
}

// applyStatement runs one statement to completion, which for a statement that enqueues an
// asynchronous DDL job means waiting for the job as well.
//
// The retry covers the enqueue alone, so each job is enqueued once: a re-run of CREATE INDEX ASYNC
// reports 42P07 for the index that now exists, and a re-run of ALTER TABLE ASYNC ... VALIDATE
// CONSTRAINT enqueues a second validation job while the first is in flight. The engine wants the
// wait out here too — CREATE INDEX ASYNC runs inside a transaction block, while CALL
// sys.wait_for_job answers 0A000 there.
//
// The wait is unconditional: Run returns once the job is finished, which is what lets the version
// migrate then marks clean describe the schema it has. Run returning is migrate's signal that the
// migration applied, and it clears the dirty flag immediately afterwards with nothing in between
// (migrate.go). Waiting is also what surfaces a build that failed, while sys.jobs still holds it —
// terminal rows survive at least 30 minutes. A failed CREATE INDEX ASYNC leaves the index INVALID,
// and a unique one goes on enforcing uniqueness until it is dropped.
func (d *DSQL) applyStatement(ctx context.Context, statement []byte) error {
	var jobID string
	err := d.retry(ctx, func() error {
		var runErr error
		jobID, runErr = d.runStatement(ctx, statement)
		return runErr
	})
	if err != nil {
		return migrationError(statement, err)
	}
	if jobID == "" {
		return nil
	}
	if err := d.awaitAsyncJob(ctx, jobID); err != nil {
		return migrationError(statement, err)
	}
	return nil
}

// migrationError adds migrate's context to a failure from one statement, reporting the line the
// server pointed at. The wrapping belongs here rather than inside the retried unit — see retry.
//
// Reporting the position matters more here than in the postgres drivers: the default path sends the
// whole file as one statement, so pgErr.Position is file-relative.
func migrationError(statement []byte, err error) error {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return database.Error{OrigErr: err, Err: "migration failed", Query: statement}
	}

	line, col, ok := computeLineFromPos(string(statement), int(pgErr.Position))
	message := fmt.Sprintf("migration failed: %s", pgErr.Message)
	if ok {
		message = fmt.Sprintf("%s (column %d)", message, col)
	}
	if pgErr.Detail != "" {
		message = fmt.Sprintf("%s, %s", message, pgErr.Detail)
	}
	return database.Error{OrigErr: err, Err: message, Query: statement, Line: line}
}

// Copied from database/pgx/v5/pgx.go, which shares the server's position reporting.
func computeLineFromPos(s string, pos int) (line uint, col uint, ok bool) {
	// replace crlf with lf
	s = strings.ReplaceAll(s, "\r\n", "\n")
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

// runStatement executes one statement and reports the job_id of the asynchronous DDL job it
// enqueued, or the empty string when it enqueued none.
//
// applyStatement wraps this in retry, so it returns the driver's error unwrapped; the
// database.Error carrying the line number belongs to whoever calls retry — see retry.
func (d *DSQL) runStatement(ctx context.Context, statement []byte) (string, error) {
	query := string(statement)
	if strings.TrimSpace(query) == "" {
		return "", nil
	}

	// Zero is "no limit", as it is in postgres and pgx, and context.WithTimeout(ctx, 0)
	// yields a context that has already expired.
	if d.config.StatementTimeout != 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, d.config.StatementTimeout)
		defer cancel()
	}

	// ExecContext cannot carry the job_id out: stdlib returns driver.RowsAffected, an int64, so
	// the row is discarded before a sql.Result exists.
	if enqueuesAsyncJob(query) {
		var jobID string
		if err := d.db.QueryRowContext(ctx, query).Scan(&jobID); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				// An index already in place: the statement did its work and enqueued nothing,
				// so long as what it found is valid.
				if name, tolerated := toleratesMissingJob(query); tolerated {
					return "", d.confirmIndexIsValid(ctx, name, query)
				}
				// Any other statement returning no row is a job this driver would never wait
				// on, which is worse than a failure, so say so rather than carrying on.
				return "", fmt.Errorf("dsql: statement looked like asynchronous DDL but returned no job_id: %s", firstLine(query))
			}
			return "", err
		}
		return jobID, nil
	}

	if _, err := d.db.ExecContext(ctx, query); err != nil {
		return "", err
	}
	return "", nil
}

var (
	// asyncIndexPattern matches CREATE [UNIQUE] INDEX ASYNC, one of the two forms that hand a
	// job_id back to the client.
	asyncIndexPattern = regexp.MustCompile(`(?is)^CREATE\s+(UNIQUE\s+)?INDEX\s+ASYNC\b`)

	// asyncIndexIfNotExistsPattern matches the IF NOT EXISTS spelling of the same statement. The
	// clause sits between ASYNC and the index name, DSQL's grammar being
	// CREATE [UNIQUE] INDEX ASYNC [[IF NOT EXISTS] name] ON table. See toleratesMissingJob.
	//
	// The name is captured for indexNameFromIfNotExists. IF NOT EXISTS always carries one, DSQL
	// answering a parse error without it, and a quoted name takes a doubled quote per quote in it.
	asyncIndexIfNotExistsPattern = regexp.MustCompile(
		`(?is)^CREATE\s+(?:UNIQUE\s+)?INDEX\s+ASYNC\s+IF\s+NOT\s+EXISTS\s+("(?:[^"]|"")+"|[^\s("]+)`)

	// asyncValidatePattern matches ALTER TABLE ASYNC ... VALIDATE CONSTRAINT, the other one.
	// Measured against a live cluster: it returns a job_id, and the wait then reports a CHECK
	// that some row violates and leaves the constraint unvalidated.
	//
	// Only this spelling does. The constraint is added with a plain ALTER TABLE ... ADD
	// CONSTRAINT ... NOT VALID, and ALTER TABLE ASYNC ... ADD CONSTRAINT and a validation
	// without ASYNC both answer 0A000 — which is why the pattern asks for ASYNC and VALIDATE
	// CONSTRAINT together rather than either on its own.
	asyncValidatePattern = regexp.MustCompile(`(?is)^ALTER\s+TABLE\s+ASYNC\b.*\bVALIDATE\s+CONSTRAINT\b`)
)

// asyncPrefixBudget bounds how much of a statement replaceComments normalizes, counted in the SQL
// it writes rather than the text it reads. The longest run either pattern has to see is
// ALTER TABLE ASYNC <table> ... VALIDATE CONSTRAINT <name>, a few hundred bytes, so this leaves room
// for many times that. Comment text falls outside the count, which is what keeps the keywords inside
// the budget however much of it runs in front of them.
//
// It matters because the default path hands Run a whole file as one statement, and rewriting ten
// megabytes of it to read the first keyword would be waste.
const asyncPrefixBudget = 8 << 10

// enqueuesAsyncJob reports whether a statement returns a job_id that has to be waited on.
//
// Comments are replaced with spaces before matching, because the server reads a comment as
// whitespace and so CREATE /* build it later */ INDEX ASYNC is the same statement as
// CREATE INDEX ASYNC. Matching the raw text classifies the commented spelling as ordinary DDL,
// and runStatement then sends it through ExecContext, which discards the job_id the server
// returned: Run reports the migration applied, migrate clears the dirty flag, and the index is
// still building. Nothing surfaces. dsql-lint accepts that spelling too, so the check the README
// recommends passes a file carrying it.
//
// Only the leading form is inspected, which is what the statement is: by the time this runs,
// either the file is one statement or the splitter has already divided it.
func enqueuesAsyncJob(statement string) bool {
	statement = strings.TrimLeft(replaceComments(statement), " \t\r\n")
	return asyncIndexPattern.MatchString(statement) || asyncValidatePattern.MatchString(statement)
}

// toleratesMissingJob reports whether a statement that returned no job_id found its object already
// in place, so it did its work rather than leaving a job unwaited on.
//
// CREATE INDEX ASYNC IF NOT EXISTS over an existing index succeeds, skips, and returns no row, which
// is what a re-run of the migration that created it does; applyStatement already reads an empty
// job_id as nothing to wait for. Only this form qualifies — ALTER TABLE ASYNC ... VALIDATE CONSTRAINT
// has no IF NOT EXISTS spelling, so a missing job_id there stays an error.
//
// The index it names comes back with the answer, because the name is what the tolerance rests on:
// confirmIndexIsValid needs it to tell an index that works from one left behind by a build that
// failed. A statement qualifies exactly when it names an index, so one read of it settles both.
func toleratesMissingJob(statement string) (string, bool) {
	name := indexNameFromIfNotExists(statement)
	return name, name != ""
}

// indexNameFromIfNotExists returns the index a CREATE INDEX ASYNC IF NOT EXISTS statement names, as
// the catalog holds it: an unquoted name folded to lower case, a quoted one unwrapped with each
// doubled quote collapsed to the one character it stands for. Empty for any other statement. Runs on
// the no-row path alone, beside toleratesMissingJob.
func indexNameFromIfNotExists(statement string) string {
	match := asyncIndexIfNotExistsPattern.FindStringSubmatch(
		strings.TrimLeft(replaceComments(statement), " \t\r\n"))
	if match == nil {
		return ""
	}
	if name, quoted := strings.CutPrefix(match[1], `"`); quoted {
		return strings.ReplaceAll(strings.TrimSuffix(name, `"`), `""`, `"`)
	}
	return strings.ToLower(match[1])
}

// indexValidityQuery reads whether a named index in the driver's schema is valid. The schema is
// named rather than left to search_path, which carries x-migrations-schema only when the URL sets
// search_path too (see poolConfigFor); an index on a table in another schema leaves no row.
const indexValidityQuery = `SELECT i.indisvalid FROM pg_index i ` +
	`JOIN pg_class c ON c.oid = i.indexrelid ` +
	`JOIN pg_namespace n ON n.oid = c.relnamespace ` +
	`WHERE c.relname = $1 AND n.nspname = $2`

// confirmIndexIsValid reports a name that resolves to an index left INVALID by a build that failed.
//
// DSQL keeps such an index rather than removing it, name and definition both, and a unique one goes
// on enforcing uniqueness until it is dropped. So IF NOT EXISTS skips and returns no row for an index
// that works and for one that only occupies its name; this separates them, leaving the re-run of a
// failed migration something to act on.
//
// A name resolving to nothing is left alone — the index is in another schema, or something moved
// underneath the statement — and the tolerant reading is the useful one.
func (d *DSQL) confirmIndexIsValid(ctx context.Context, name, statement string) error {
	var valid bool
	if err := d.db.QueryRowContext(ctx, indexValidityQuery, name, d.config.SchemaName).Scan(&valid); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		// Unwrapped: inside the retried unit a database.Error hides the *pgconn.PgError from
		// IsOCCError, and a catalog read just after a CREATE INDEX is where an OC001 lands. The
		// retry re-runs the statement, which skips again. See retry.
		return err
	}
	if !valid {
		return fmt.Errorf(
			"dsql: index %s is already there and INVALID from a build that failed, so this statement did nothing: drop the index and run the migration again: %s",
			name, firstLine(statement))
	}
	return nil
}

// replaceComments returns the statement with its leading SQL comments replaced by a single space
// each, leaving the keywords around them adjacent for the patterns above to match. It normalizes
// until it has written asyncPrefixBudget bytes of SQL and hands the rest over as it stands.
// Whitespace is left alone, both patterns already accepting runs of it.
//
// Block comments nest, as they do in PostgreSQL, so /* a /* b */ c */ is one comment rather than a
// comment followed by stray text. An unterminated comment of either kind runs to the end of the
// statement, which is the reading the server would report an error for anyway.
//
// String literals get no special handling. Both patterns are anchored at ^, so a match is decided
// by the statement's opening keywords and no literal can sit in front of them. One further along
// that happens to contain /* is rewritten in this copy alone, never in the statement the driver
// executes.
func replaceComments(statement string) string {
	head := min(len(statement), asyncPrefixBudget)
	// A comment further along than the patterns reach changes no verdict.
	if !strings.Contains(statement[:head], "--") && !strings.Contains(statement[:head], "/*") {
		return statement
	}

	var out strings.Builder
	out.Grow(len(statement))
	// sql counts the bytes written outside a comment, so a comment costs its one space and nothing
	// against the budget. Counting the input consumed instead let a long enough run of comments
	// spend the whole budget and leave the keywords behind it unnormalized, classifying async DDL
	// as ordinary. Every pass consumes at least one byte, so the loop stays one pass over the
	// statement.
	i, sql := 0, 0
	for i < len(statement) && sql < asyncPrefixBudget {
		switch {
		case strings.HasPrefix(statement[i:], "--"):
			out.WriteByte(' ')
			end := strings.IndexByte(statement[i:], '\n')
			if end < 0 {
				return out.String()
			}
			i += end + 1

		case strings.HasPrefix(statement[i:], "/*"):
			out.WriteByte(' ')
			i = endOfBlockComment(statement, i)

		default:
			out.WriteByte(statement[i])
			i++
			sql++
		}
	}
	// Past the budget the text goes over as it stands, still matched on but not normalized, the
	// patterns having asyncPrefixBudget bytes of normalized SQL to read by then.
	out.WriteString(statement[i:])
	return out.String()
}

// endOfBlockComment returns the index just past the block comment opening at start, counting
// nested openings, or len(statement) when the comment is never closed.
func endOfBlockComment(statement string, start int) int {
	depth := 0
	for i := start; i < len(statement); {
		switch {
		case strings.HasPrefix(statement[i:], "/*"):
			depth++
			i += 2
		case strings.HasPrefix(statement[i:], "*/"):
			depth--
			i += 2
			if depth == 0 {
				return i
			}
		default:
			i++
		}
	}
	return len(statement)
}

// firstLine names a statement in an error message without reproducing the whole file, which
// on the default path is what a statement is.
func firstLine(statement string) string {
	statement = strings.TrimSpace(statement)
	if end := strings.IndexByte(statement, '\n'); end >= 0 {
		statement = strings.TrimSpace(statement[:end]) + " ..."
	}
	return statement
}

// awaitAsyncJob blocks until an enqueued asynchronous DDL job finishes. See applyStatement for
// why the wait is unconditional and why it sits outside the retry.
//
// sys.jobs is cluster-wide, so the job_id stays usable from whichever pooled connection the
// wait lands on.
func (d *DSQL) awaitAsyncJob(ctx context.Context, jobID string) error {
	// Called with no transaction of its own, which the procedure requires: inside one it
	// answers 0A000.
	//
	// The procedure blocks until the job reaches a terminal state, and nothing here bounds it.
	// Run takes no context, so ctx is Background; x-statement-timeout is applied in
	// runStatement, so it covers the enqueue rather than this wait. That leaves the 60-minute
	// connection limit as the only ceiling on an index build that never finishes.
	query := `CALL sys.wait_for_job($1)`
	var succeeded bool
	if err := d.db.QueryRowContext(ctx, query, jobID).Scan(&succeeded); err != nil {
		return fmt.Errorf("dsql: waiting for asynchronous DDL job %s: %w", jobID, err)
	}
	if succeeded {
		return nil
	}

	// A false is the job's own outcome and raises nothing, so the reason has to be read from
	// sys.jobs: status tells failed from canceled, and details carries the engine's own
	// message, a duplicate-key report for a unique index build that found one. Terminal rows
	// are pruned once they are over 30 minutes old, when the cluster next runs an asynchronous
	// task, so a row that has aged out leaves only the job id to report.
	var status, details sql.NullString
	if err := d.db.QueryRowContext(ctx, `SELECT status, details FROM sys.jobs WHERE job_id = $1`, jobID).Scan(&status, &details); err != nil {
		return fmt.Errorf("dsql: asynchronous DDL job %s did not succeed, and its status could not be read: %w", jobID, err)
	}
	if details.String != "" {
		return fmt.Errorf("dsql: asynchronous DDL job %s %s: %s", jobID, status.String, details.String)
	}
	return fmt.Errorf("dsql: asynchronous DDL job %s %s", jobID, status.String)
}

// SetVersion records the migration version. Migrate calls this before and after every
// migration, and Force routes through it too, so it sits on the recovery path as well as
// the happy one.
func (d *DSQL) SetVersion(version int, dirty bool) error {
	ctx := context.Background()
	table := d.qualifiedTable(d.config.MigrationsTable)

	// A DELETE with no WHERE clause, because DSQL has no TRUNCATE. Semantics are identical:
	// the table holds one row. Two DML statements and no DDL, which DSQL permits in one
	// transaction.
	deleteQuery := `DELETE FROM ` + table
	insertQuery := `INSERT INTO ` + table + ` (version, dirty) VALUES ($1, $2)`

	// One retry covers begin/delete/insert/commit as a unit: a pooled session can take OC001
	// at any of them after a schema change, and the pair is only meaningful applied together.
	// Each attempt begins its own transaction, so a conflict leaves nothing behind to undo.
	var failed string
	err := d.retry(ctx, func() error {
		failed = ""
		tx, err := d.db.BeginTx(ctx, &sql.TxOptions{})
		if err != nil {
			return err
		}

		if _, err := tx.ExecContext(ctx, deleteQuery); err != nil {
			failed = deleteQuery
			return errors.Join(err, tx.Rollback())
		}

		// Also re-write the schema version for nil dirty versions to prevent
		// empty schema version for failed down migration on the first migration
		// See: https://github.com/golang-migrate/migrate/issues/330
		if version >= 0 || (version == database.NilVersion && dirty) {
			if _, err := tx.ExecContext(ctx, insertQuery, version, dirty); err != nil {
				failed = insertQuery
				return errors.Join(err, tx.Rollback())
			}
		}

		return tx.Commit()
	})
	if err != nil {
		return &database.Error{OrigErr: err, Query: []byte(failed)}
	}
	return nil
}

func (d *DSQL) Version() (version int, dirty bool, err error) {
	ctx := context.Background()
	query := `SELECT version, dirty FROM ` + d.qualifiedTable(d.config.MigrationsTable) + ` LIMIT 1`

	// Retried so x-occ-max-retries reaches the read the OCCMaxRetries note describes: migrate
	// calls this one first, straight after Open has issued two DDLs, which is when a pooled
	// session most often answers OC001 from a stale catalog.
	//
	// Classification stays below the retry, where database.Error can wrap the result — see
	// retry(). occretry hands sql.ErrNoRows and an undefined table back on the first attempt,
	// both being outside its OCC codes, so both arms still read them.
	err = d.retry(ctx, func() error {
		return d.db.QueryRowContext(ctx, query).Scan(&version, &dirty)
	})
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return database.NilVersion, false, nil

	case err != nil:
		// Drop removes the version table, and migrate reads the version afterwards.
		if isUndefinedTable(err) {
			return database.NilVersion, false, nil
		}
		return 0, false, &database.Error{OrigErr: err, Query: []byte(query)}

	default:
		return version, dirty, nil
	}
}

// Catalog queries Drop enumerates the schema with. Each is restricted to this driver's schema —
// unlike the postgres drivers, which read current_schema(). x-migrations-schema can point
// somewhere other than the session's schema, and dropping the wrong one would be unrecoverable.
//
// These five classes are every class DSQL accepts a CREATE for: TYPE (enum and composite),
// PROCEDURE, MATERIALIZED VIEW, a plpgsql FUNCTION, EXTENSION, TEMP TABLE, AGGREGATE and COLLATION
// all answer 0A000, so no sixth class can be left behind. Only LANGUAGE sql functions are accepted,
// which is why routines are enumerated at all.
//
// Tables alone are not enough even though DROP TABLE ... CASCADE takes dependents with it: a view
// over no table, a sequence created standalone or with OWNED BY NONE, a domain and a function all
// outlive it, and each makes a re-run of the migration that created it fail — a leftover domain
// reports 42710.
//
// DSQL populates information_schema.sequences even though pg_sequences is unsupported, and pg_views
// works. Functions come from pg_proc rather than information_schema.routines because DROP FUNCTION
// needs the identity arguments to name an overload, which pg_get_function_identity_arguments
// produces.
const (
	dropViewsQuery     = `SELECT table_name FROM information_schema.views WHERE table_schema = $1`
	dropTablesQuery    = `SELECT table_name FROM information_schema.tables WHERE table_schema = $1 AND table_type = 'BASE TABLE'`
	dropSequencesQuery = `SELECT sequence_name FROM information_schema.sequences WHERE sequence_schema = $1`

	// Two columns, read by schemaDomainNames: each domain with the domain in this schema it is built
	// on, empty when its base is an ordinary type. A domain can be built on another and DROP DOMAIN
	// takes no CASCADE here, so that base has to go after it. It comes from pg_type because
	// information_schema.domains reports the chain flattened to its underlying type, while
	// typbasetype names the immediate base.
	dropDomainsQuery = `SELECT t.typname, COALESCE(b.typname, '') ` +
		`FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace ` +
		`LEFT JOIN pg_type b ON b.oid = t.typbasetype AND b.typtype = 'd' AND b.typnamespace = n.oid ` +
		`WHERE t.typtype = 'd' AND n.nspname = $1`

	// Two columns, so this one is read by schemaFunctionSignatures rather than schemaObjectNames.
	dropFunctionsQuery = `SELECT p.proname, pg_get_function_identity_arguments(p.oid) ` +
		`FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = $1`
)

// schemaObjects is what Drop found in the schema, by class. functions hold a quoted name and its
// identity arguments together, ready to name in a DROP.
type schemaObjects struct {
	views     []string
	tables    []string
	sequences []string
	domains   []string
	functions []string
}

// Drop deletes everything in the schema.
func (d *DSQL) Drop() error {
	ctx := context.Background()

	// Every name is collected before any DDL runs, rather than dropping as we read: the DROPs
	// change the catalog these queries are reading.
	objects, err := d.schemaObjectsIn(ctx)
	if err != nil {
		return err
	}

	for _, query := range d.dropStatements(objects) {
		if err := d.retry(ctx, func() error {
			_, err := d.db.ExecContext(ctx, query)
			return err
		}); err != nil {
			return &database.Error{OrigErr: err, Query: []byte(query)}
		}
	}

	// Gated on the lock table having actually been among the names dropped, rather than on reaching
	// here, so a change to the catalog queries above cannot quietly turn Unlock into a no-op that
	// leaves a row behind.
	if len(lockTableIfPresent(objects.tables, d.config.LockTable)) > 0 {
		d.lockTableDropped.Store(true)
	}

	return nil
}

// schemaObjectsIn enumerates the schema Drop is pointed at.
func (d *DSQL) schemaObjectsIn(ctx context.Context) (schemaObjects, error) {
	var (
		objects schemaObjects
		err     error
	)
	if objects.views, err = d.schemaObjectNames(ctx, dropViewsQuery); err != nil {
		return objects, err
	}
	if objects.tables, err = d.schemaObjectNames(ctx, dropTablesQuery); err != nil {
		return objects, err
	}
	if objects.sequences, err = d.schemaObjectNames(ctx, dropSequencesQuery); err != nil {
		return objects, err
	}
	if objects.domains, err = d.schemaDomainNames(ctx); err != nil {
		return objects, err
	}
	if objects.functions, err = d.schemaFunctionSignatures(ctx); err != nil {
		return objects, err
	}
	return objects, nil
}

// dropStatements orders the DROPs Drop issues: views, tables, sequences, functions, domains, and
// the lock table last of all.
//
// Views lead, so a view is removed as itself rather than by CASCADE from under a table. Sequences
// follow the tables, a sequence being ownable by a column of a table still standing. Functions
// precede domains because a function's parameter or return type can be a domain and DROP DOMAIN is
// the one drop here carrying no CASCADE — DSQL answers 0A000 to it, so a domain whose turn comes
// while such a function stands reports 2BP01 and ends the run. Taking the functions first settles
// the other direction too, a domain's CHECK being able to call one that CASCADE then carries off.
//
// The lock table last is what keeps the lock held for the whole run: a DROP that fails returns with
// the lock table standing, so the row this instance holds is there for the Unlock that Migrate
// issues next. Nothing else in the schema depends on it, so its turn coming last costs nothing, and
// reaching the end means its own DROP succeeded — the flag Drop then sets is what suppresses that
// Unlock.
//
// IF EXISTS on each, so a CASCADE that took an object before its own turn came does not fail the
// run, and CASCADE on each but DROP DOMAIN: by a domain's turn every view, table and function is
// gone, and the domains arrive ordered among themselves, one built on another ahead of its base
// (schemaDomainNames), so nothing is left for a cascade to reach.
func (d *DSQL) dropStatements(objects schemaObjects) []string {
	schema := quoteIdentifier(d.config.SchemaName)
	drops := make([]string, 0, len(objects.views)+len(objects.tables)+
		len(objects.sequences)+len(objects.domains)+len(objects.functions))

	for _, name := range objects.views {
		drops = append(drops, `DROP VIEW IF EXISTS `+d.qualifiedTable(name)+` CASCADE`)
	}
	for _, name := range withoutTable(objects.tables, d.config.LockTable) {
		drops = append(drops, `DROP TABLE IF EXISTS `+d.qualifiedTable(name)+` CASCADE`)
	}
	for _, name := range objects.sequences {
		drops = append(drops, `DROP SEQUENCE IF EXISTS `+d.qualifiedTable(name)+` CASCADE`)
	}
	for _, signature := range objects.functions {
		drops = append(drops, `DROP FUNCTION IF EXISTS `+schema+`.`+signature+` CASCADE`)
	}
	for _, name := range objects.domains {
		drops = append(drops, `DROP DOMAIN IF EXISTS `+d.qualifiedTable(name))
	}
	for _, name := range lockTableIfPresent(objects.tables, d.config.LockTable) {
		drops = append(drops, `DROP TABLE IF EXISTS `+d.qualifiedTable(name)+` CASCADE`)
	}

	return drops
}

// schemaObjectNames runs one of the single-column catalog queries above and collects the names it
// returns.
func (d *DSQL) schemaObjectNames(ctx context.Context, query string) ([]string, error) {
	rows, err := d.db.QueryContext(ctx, query, d.config.SchemaName)
	if err != nil {
		return nil, &database.Error{OrigErr: err, Query: []byte(query)}
	}

	names := make([]string, 0)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, errors.Join(err, rows.Close())
		}
		if name != "" {
			names = append(names, name)
		}
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, &database.Error{OrigErr: err, Query: []byte(query)}
	}
	return names, nil
}

// schemaFunctionSignatures collects each function in the schema as a quoted name followed by its
// identity arguments, which is what DROP FUNCTION needs to name one of several overloads. The
// arguments go over as the server rendered them, since it is the server that has to parse them
// back.
func (d *DSQL) schemaFunctionSignatures(ctx context.Context) ([]string, error) {
	rows, err := d.db.QueryContext(ctx, dropFunctionsQuery, d.config.SchemaName)
	if err != nil {
		return nil, &database.Error{OrigErr: err, Query: []byte(dropFunctionsQuery)}
	}

	signatures := make([]string, 0)
	for rows.Next() {
		var name, arguments string
		if err := rows.Scan(&name, &arguments); err != nil {
			return nil, errors.Join(err, rows.Close())
		}
		if name != "" {
			signatures = append(signatures, quoteIdentifier(name)+`(`+arguments+`)`)
		}
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, &database.Error{OrigErr: err, Query: []byte(dropFunctionsQuery)}
	}
	return signatures, nil
}

// schemaDomainNames lists the schema's domains, each one ahead of the domain it is built on.
func (d *DSQL) schemaDomainNames(ctx context.Context) ([]string, error) {
	rows, err := d.db.QueryContext(ctx, dropDomainsQuery, d.config.SchemaName)
	if err != nil {
		return nil, &database.Error{OrigErr: err, Query: []byte(dropDomainsQuery)}
	}

	names := make([]string, 0)
	bases := make(map[string]string)
	for rows.Next() {
		var name, base string
		if err := rows.Scan(&name, &base); err != nil {
			return nil, errors.Join(err, rows.Close())
		}
		if name != "" {
			names = append(names, name)
			bases[name] = base
		}
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, &database.Error{OrigErr: err, Query: []byte(dropDomainsQuery)}
	}
	return domainsDependentsFirst(names, bases), nil
}

// domainsDependentsFirst orders domains by how long a chain of domains each sits on top of, deepest
// first, which puts every domain ahead of the one it is built on. The sort is stable, so domains of
// equal depth keep the catalog's order and Drop issues the same statements between runs.
//
// The walk is capped at the number of domains read. A cycle cannot reach the catalog, the DDL that
// would make one being rejected, so the cap is there to keep a surprising read finite.
func domainsDependentsFirst(names []string, bases map[string]string) []string {
	depths := make(map[string]int, len(names))
	for _, name := range names {
		depth := 0
		for base := bases[name]; base != "" && depth <= len(names); base = bases[base] {
			depth++
		}
		depths[name] = depth
	}

	ordered := slices.Clone(names)
	slices.SortStableFunc(ordered, func(a, b string) int { return depths[b] - depths[a] })
	return ordered
}

// withoutTable returns names with one entry removed, and lockTableIfPresent returns that
// entry. Together they order the lock table last without mutating the slice Drop read from
// the catalog.
func withoutTable(names []string, exclude string) []string {
	kept := make([]string, 0, len(names))
	for _, name := range names {
		if name != exclude {
			kept = append(kept, name)
		}
	}
	return kept
}

func lockTableIfPresent(names []string, lockTable string) []string {
	for _, name := range names {
		if name == lockTable {
			return []string{lockTable}
		}
	}
	return nil
}

// ensureVersionTable creates the version table. Like cockroachdb's, it takes the lock
// itself, which deviates from the usual "caller locks" convention in this type.
//
// lock(false) rather than Lock: Open runs this, and every command reaches Open — migrate version
// among them — so forcing here would release a lock a migration in flight is holding. x-force-lock
// stays with the Lock migrate itself calls.
//
// A row already there is another migration creating this table or already past it, so the lock stays
// with it and Open carries on, which is what lets the commands taking no lock of their own run
// against a migrating database. The table being absent meanwhile reads as NilVersion — see Version.
func (d *DSQL) ensureVersionTable() (err error) {
	if err := d.lock(false); err != nil {
		if errors.Is(err, database.ErrLocked) {
			return nil
		}
		return err
	}
	// Joined onto the return rather than discarded, because the lock is a durable row and
	// releasing it is what leaves the next run free to take it. migrate force takes the lock
	// like any other command, so x-force-lock is what releases a row left behind.
	defer func() {
		if e := d.Unlock(); e != nil {
			err = errors.Join(err, e)
		}
	}()

	// No existence check ahead of this, for the same reason as ensureLockTable.
	query := `CREATE TABLE IF NOT EXISTS ` + d.qualifiedTable(d.config.MigrationsTable) +
		` (version BIGINT NOT NULL PRIMARY KEY, dirty BOOLEAN NOT NULL)`
	if err := d.retryAtLeast(context.Background(), bootstrapRetries, func() error {
		_, err := d.db.ExecContext(context.Background(), query)
		return err
	}); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}
	return nil
}

// ensureLockTable creates the lock table. It runs before the lock exists, so it cannot be
// lock-protected, and is written to be safe under concurrency instead.
func (d *DSQL) ensureLockTable() error {
	// CREATE TABLE IF NOT EXISTS with no existence check ahead of it, which would reintroduce the
	// race the retry closes. PostgreSQL needs that check, IF NOT EXISTS having a genuine
	// non-transactional window there; DSQL's catalog is transactional under OCC, so a losing
	// concurrent creation surfaces as 40001 at commit rather than as 42P07.
	//
	// The floor is bootstrapRetries rather than OCCMaxRetries, this running before the caller's
	// opt-in setting can protect anything.
	query := `CREATE TABLE IF NOT EXISTS ` + d.qualifiedTable(d.config.LockTable) +
		` (lock_id TEXT NOT NULL PRIMARY KEY)`
	if err := d.retryAtLeast(context.Background(), bootstrapRetries, func() error {
		_, err := d.db.ExecContext(context.Background(), query)
		return err
	}); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}
	return nil
}

func (d *DSQL) qualifiedTable(table string) string {
	return quoteIdentifier(d.config.SchemaName) + "." + quoteIdentifier(table)
}

func isUndefinedTable(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.SQLState() == pgerrcode.UndefinedTable
}

// Copied from lib/pq implementation: https://github.com/lib/pq/blob/v1.9.0/conn.go#L1611
func quoteIdentifier(name string) string {
	if end := strings.IndexRune(name, 0); end >= 0 {
		name = name[:end]
	}
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}
