package dsql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"maps"
	nurl "net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	awsdsql "github.com/awslabs/aurora-dsql-connectors/go/pgx/dsql"
	"github.com/awslabs/aurora-dsql-connectors/go/pgx/occretry"
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
)

const (
	testSchema   = "public"
	testDatabase = "postgres"

	// userTable and trivialStatement stand in for a caller's own table and a statement that
	// enqueues nothing, wherever the test is about something else.
	userTable        = "users"
	trivialStatement = "SELECT 1"
)

func mustParseConfig(t *testing.T, rawURL string) *Config {
	t.Helper()
	purl, err := nurl.Parse(rawURL)
	if err != nil {
		t.Fatalf("parsing %q: %v", rawURL, err)
	}
	config, err := parseConfig(purl)
	if err != nil {
		t.Fatalf("parseConfig(%q): %v", rawURL, err)
	}
	return config
}

func TestParseConfigDefaults(t *testing.T) {
	config := mustParseConfig(t, "dsql://admin@cluster.dsql.us-east-1.on.aws:5432/postgres")

	if config.DatabaseName != testDatabase {
		t.Errorf("DatabaseName = %q, want postgres", config.DatabaseName)
	}
	if config.MigrationsTable != "" {
		t.Errorf("MigrationsTable = %q, want empty before setDefaults", config.MigrationsTable)
	}
	// Left empty so WithInstance can fall back to CURRENT_SCHEMA().
	if config.SchemaName != "" {
		t.Errorf("SchemaName = %q, want empty when x-migrations-schema is absent", config.SchemaName)
	}
	if config.ForceLock {
		t.Error("ForceLock = true, want false")
	}
	if config.MultiStatementEnabled {
		t.Error("MultiStatementEnabled = true, want false")
	}
	if config.StatementTimeout != 0 {
		t.Errorf("StatementTimeout = %v, want 0", config.StatementTimeout)
	}
	if config.MultiStatementMaxSize != DefaultMultiStatementMaxSize {
		t.Errorf("MultiStatementMaxSize = %d, want %d", config.MultiStatementMaxSize, DefaultMultiStatementMaxSize)
	}
	// Retry is opt-in: the driver imposes no count of its own.
	if config.OCCMaxRetries != 0 {
		t.Errorf("OCCMaxRetries = %d, want 0 when x-occ-max-retries is absent", config.OCCMaxRetries)
	}
	if config.OCCMaxRetryDelay != DefaultOCCMaxRetryDelay {
		t.Errorf("OCCMaxRetryDelay = %v, want %v", config.OCCMaxRetryDelay, DefaultOCCMaxRetryDelay)
	}
}

func TestParseConfigAllOptions(t *testing.T) {
	config := mustParseConfig(t, "dsql://admin@cluster.dsql.us-east-1.on.aws/mydb?"+
		"x-migrations-schema=app&x-migrations-table=mt&x-lock-table=lt&x-force-lock=true&"+
		"x-statement-timeout=250&"+
		"x-multi-statement=true&x-multi-statement-max-size=4096&"+
		"x-occ-max-retries=7&x-occ-max-retry-delay=1500&search_path=app")

	// search_path has no Config field: Open applies it to the pool, so all this pins is
	// that parseConfig accepts it.

	if config.DatabaseName != "mydb" {
		t.Errorf("DatabaseName = %q, want mydb", config.DatabaseName)
	}
	if config.SchemaName != "app" {
		t.Errorf("SchemaName = %q, want app", config.SchemaName)
	}
	if config.MigrationsTable != "mt" {
		t.Errorf("MigrationsTable = %q, want mt", config.MigrationsTable)
	}
	if config.LockTable != "lt" {
		t.Errorf("LockTable = %q, want lt", config.LockTable)
	}
	if !config.ForceLock {
		t.Error("ForceLock = false, want true")
	}
	if config.StatementTimeout != 250*time.Millisecond {
		t.Errorf("StatementTimeout = %v, want 250ms", config.StatementTimeout)
	}
	if !config.MultiStatementEnabled {
		t.Error("MultiStatementEnabled = false, want true")
	}
	if config.MultiStatementMaxSize != 4096 {
		t.Errorf("MultiStatementMaxSize = %d, want 4096", config.MultiStatementMaxSize)
	}
	if config.OCCMaxRetries != 7 {
		t.Errorf("OCCMaxRetries = %d, want 7", config.OCCMaxRetries)
	}
	if config.OCCMaxRetryDelay != 1500*time.Millisecond {
		t.Errorf("OCCMaxRetryDelay = %v, want 1.5s", config.OCCMaxRetryDelay)
	}
}

// A malformed option must fail loudly rather than fall back to a default, per
// database/driver.go: "Don't try to correct user input. Don't assume things."
func TestParseConfigRejectsBadOptions(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		wantText string
	}{
		{"statement timeout", "x-statement-timeout=soon", "x-statement-timeout"},
		{"multi statement max size", "x-multi-statement-max-size=big", "x-multi-statement-max-size"},
		{"occ max retries", "x-occ-max-retries=lots", "x-occ-max-retries"},
		{"occ max retry delay", "x-occ-max-retry-delay=later", "x-occ-max-retry-delay"},
		{"force lock", "x-force-lock=maybe", "x-force-lock"},
		{"multi statement", "x-multi-statement=maybe", "x-multi-statement"},
		{"negative occ retries", "x-occ-max-retries=-1", "x-occ-max-retries"},
		{"negative occ retry delay", "x-occ-max-retry-delay=-1", "x-occ-max-retry-delay"},

		// Silently ignored is worse than refused: the operator believes the setting took.
		// This spelling carries arbitrary -c flags alongside the schema list, and it is the
		// one the Aurora DSQL ORM integrations tell users to write, so the error names the
		// spelling this driver applies.
		{"options search_path", "options=-c%20search_path%3Dapp", "search_path=app"},
		{"migrations table quoted", "x-migrations-table-quoted=true", "x-migrations-table-quoted"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			purl, err := nurl.Parse("dsql://admin@host/postgres?" + test.query)
			if err != nil {
				t.Fatalf("parsing URL: %v", err)
			}
			_, err = parseConfig(purl)
			if err == nil {
				t.Fatalf("parseConfig(%q) succeeded, want an error", test.query)
			}
			if !strings.Contains(err.Error(), test.wantText) {
				t.Errorf("error %q does not name the offending option %q", err, test.wantText)
			}
		})
	}
}

// A non-positive size means "unset", not "zero-length buffer".
func TestParseConfigNonPositiveMaxSizeFallsBack(t *testing.T) {
	for _, value := range []string{"0", "-1"} {
		config := mustParseConfig(t, "dsql://admin@host/postgres?x-multi-statement-max-size="+value)
		if config.MultiStatementMaxSize != DefaultMultiStatementMaxSize {
			t.Errorf("size %s: got %d, want the default %d", value, config.MultiStatementMaxSize, DefaultMultiStatementMaxSize)
		}
	}
}

func TestConfigSetDefaults(t *testing.T) {
	config := &Config{}
	config.setDefaults()

	if config.MigrationsTable != DefaultMigrationsTable {
		t.Errorf("MigrationsTable = %q, want %q", config.MigrationsTable, DefaultMigrationsTable)
	}
	if config.LockTable != DefaultLockTable {
		t.Errorf("LockTable = %q, want %q", config.LockTable, DefaultLockTable)
	}
	if config.MultiStatementMaxSize != DefaultMultiStatementMaxSize {
		t.Errorf("MultiStatementMaxSize = %d, want %d", config.MultiStatementMaxSize, DefaultMultiStatementMaxSize)
	}
	if config.OCCMaxRetryDelay != DefaultOCCMaxRetryDelay {
		t.Errorf("OCCMaxRetryDelay = %v, want %v", config.OCCMaxRetryDelay, DefaultOCCMaxRetryDelay)
	}

	// OCCMaxRetries is left alone: zero is the connector's "no retry" and defaulting it
	// here would make that value unreachable from a Config literal.
	if config.OCCMaxRetries != 0 {
		t.Errorf("OCCMaxRetries = %d, want 0 left untouched", config.OCCMaxRetries)
	}
}

func TestOCCConfigMapping(t *testing.T) {
	config := &Config{OCCMaxRetries: 9, OCCMaxRetryDelay: 3 * time.Second}
	occ := config.occConfig()

	if occ.MaxRetries != 9 {
		t.Errorf("MaxRetries = %d, want 9", occ.MaxRetries)
	}
	if occ.MaxWait != 3*time.Second {
		t.Errorf("MaxWait = %v, want 3s", occ.MaxWait)
	}
	// Unset fields keep the connector's defaults rather than becoming zero.
	if occ.InitialWait <= 0 {
		t.Errorf("InitialWait = %v, want the connector default", occ.InitialWait)
	}
	if occ.Multiplier <= 1 {
		t.Errorf("Multiplier = %v, want the connector default", occ.Multiplier)
	}
}

// The retry count is the connector's value, not a reinterpretation of it, and zero is both
// the driver's default and occretry's "run once". So retry is opt-in and the two entry
// points cannot disagree, which is the thing a non-zero default could not deliver.
func TestOCCMaxRetriesIsPassedThrough(t *testing.T) {
	tests := []struct {
		query string
		want  int
	}{
		{"", 0},
		{"?x-occ-max-retries=", 0},
		{"?x-occ-max-retries=0", 0},
		{"?x-occ-max-retries=7", 7},
	}

	for _, test := range tests {
		t.Run(test.query, func(t *testing.T) {
			config := mustParseConfig(t, "dsql://admin@host/postgres"+test.query)
			config.setDefaults()
			if got := config.occConfig().MaxRetries; got != test.want {
				t.Errorf("MaxRetries = %d, want %d", got, test.want)
			}
		})
	}

	// A bare URL and a bare Config literal must resolve identically, which a non-zero default on
	// the URL side cannot manage: a Config literal's zero is indistinguishable from "unset".
	fromURL := mustParseConfig(t, "dsql://admin@host/postgres")
	fromURL.setDefaults()
	literal := &Config{}
	literal.setDefaults()
	if got, want := literal.occConfig().MaxRetries, fromURL.occConfig().MaxRetries; got != want {
		t.Errorf("&Config{} gives MaxRetries = %d, but a bare URL gives %d", got, want)
	}
}

// occretry has no normalization of its own, so occConfig has to seed from DefaultConfig or
// Multiplier and MaxWait arrive as zero. MaxWait additionally needs a floor: the connector
// clamps each wait to it and computes jitter as rand.Int63n(wait/4), which panics below 4ns.
func TestOCCConfigKeepsConnectorSafe(t *testing.T) {
	tests := []struct {
		name   string
		config *Config
	}{
		{"unset", &Config{}},
		{"negative delay", &Config{OCCMaxRetryDelay: -time.Second}},
		{"sub-nanosecond delay", &Config{OCCMaxRetryDelay: 3}},
		{"one nanosecond delay", &Config{OCCMaxRetries: 5, OCCMaxRetryDelay: 1}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.config.setDefaults()
			occ := test.config.occConfig()

			if occ.Multiplier <= 1 {
				t.Errorf("Multiplier = %v, want the connector default", occ.Multiplier)
			}
			if occ.MaxWait < minOCCRetryDelay {
				t.Errorf("MaxWait = %v, want at least %v to keep the connector's jitter safe", occ.MaxWait, minOCCRetryDelay)
			}
			if occ.InitialWait > occ.MaxWait {
				t.Errorf("InitialWait = %v exceeds MaxWait = %v, so the first backoff ignores the bound", occ.InitialWait, occ.MaxWait)
			}
			if occ.InitialWait <= 0 {
				t.Errorf("InitialWait = %v, want a positive seed", occ.InitialWait)
			}
		})
	}
}

// A password is rejected, not ignored. The connector discards one and authenticates with
// IAM, so accepting it would leave an operator believing a credential was in use.
func TestParseConfigRejectsPassword(t *testing.T) {
	rejected := []string{
		"dsql://admin:hunter2@cluster.dsql.us-east-1.on.aws:5432/postgres",
		"dsql://admin:@cluster.dsql.us-east-1.on.aws/postgres", // present but empty
		"dsql://:hunter2@cluster.dsql.us-east-1.on.aws/postgres",
		// The query spelling reaches the connector, which has no password parameter to
		// read it with, so it is refused here as well.
		"dsql://admin@cluster.dsql.us-east-1.on.aws/postgres?password=hunter2",
		"dsql://admin@cluster.dsql.us-east-1.on.aws/postgres?password=", // present but empty
	}
	for _, rawURL := range rejected {
		t.Run(rawURL, func(t *testing.T) {
			purl, err := nurl.Parse(rawURL)
			if err != nil {
				t.Fatalf("parsing %q: %v", rawURL, err)
			}
			if _, err := parseConfig(purl); !errors.Is(err, ErrPasswordSet) {
				t.Errorf("parseConfig(%q) = %v, want ErrPasswordSet", rawURL, err)
			}
		})
	}

	accepted := []string{
		"dsql://admin@cluster.dsql.us-east-1.on.aws:5432/postgres",
		"dsql://cluster.dsql.us-east-1.on.aws/postgres", // no userinfo at all
	}
	for _, rawURL := range accepted {
		t.Run(rawURL, func(t *testing.T) {
			purl, err := nurl.Parse(rawURL)
			if err != nil {
				t.Fatalf("parsing %q: %v", rawURL, err)
			}
			if _, err := parseConfig(purl); err != nil {
				t.Errorf("parseConfig(%q) = %v, want success", rawURL, err)
			}
		})
	}
}

// database.Error has no Unwrap, so anything wrapped in one is invisible to errors.As and
// errors.Is. That is why retry() takes the raw error and migrate's context is added only
// after it returns — see retry(). Left as a test because the trap is silent: wrapping in
// the wrong order degrades retry to a single attempt without any symptom.
func TestDatabaseErrorHidesItsCause(t *testing.T) {
	pgErr := &pgconn.PgError{Code: "OC001", Message: "schema has been updated by another transaction"}

	for _, wrapped := range []error{
		database.Error{OrigErr: pgErr},
		&database.Error{OrigErr: pgErr},
	} {
		var found *pgconn.PgError
		if errors.As(wrapped, &found) {
			t.Errorf("%T now unwraps; retry() may stop taking the raw error", wrapped)
		}
		if occretry.IsOCCError(wrapped) {
			t.Errorf("%T: the connector can now classify a wrapped error", wrapped)
		}
	}

	// Unwrapped, the same error is retryable — the ordering is the whole difference.
	if !occretry.IsOCCError(pgErr) {
		t.Error("a bare OC001 is not classified as an OCC error")
	}
}

// The hook writes the raw value into RuntimeParams, which is where pgx keeps a search_path from a
// URL, so the server parses it as it would through postgres or pgx — see poolConfigFor.
func TestSearchPathHookPreservesRawValue(t *testing.T) {
	for _, searchPath := range []string{"app", "APP", `"App", public`, "app, public", "$user, public"} {
		poolConfig, err := poolConfigFor(searchPath)
		if err != nil {
			t.Fatalf("poolConfigFor(%q): %v", searchPath, err)
		}
		if poolConfig.BeforeConnect == nil {
			t.Fatalf("BeforeConnect is nil for %q, so search_path would never reach the server", searchPath)
		}

		// A copy, as pgxpool passes the hook.
		connConfig := poolConfig.ConnConfig.Copy()
		if err := poolConfig.BeforeConnect(context.Background(), connConfig); err != nil {
			t.Fatalf("BeforeConnect(%q): %v", searchPath, err)
		}
		if got := connConfig.RuntimeParams["search_path"]; got != searchPath {
			t.Errorf("RuntimeParams[search_path] = %q, want %q verbatim", got, searchPath)
		}
	}
}

// Supplying a pool config turns off the connector's own defaults: it fills the two lifetimes
// only when they are zero, and pgxpool.ParseConfig has already set them to 1h and 30m. An
// hour is exactly when DSQL closes a connection server-side, so leaving them would hand out
// connections the server is about to drop. Asserted because nothing else would notice: the
// pool still works, it just stops recycling ahead of the server.
func TestPoolConfigPinsConnectorLifetimes(t *testing.T) {
	poolConfig, err := poolConfigFor("")
	if err != nil {
		t.Fatalf("poolConfigFor: %v", err)
	}

	if poolConfig.MaxConnLifetime != awsdsql.DefaultMaxConnLifetime {
		t.Errorf("MaxConnLifetime = %v, want the connector's %v",
			poolConfig.MaxConnLifetime, awsdsql.DefaultMaxConnLifetime)
	}
	if poolConfig.MaxConnIdleTime != awsdsql.DefaultMaxConnIdleTime {
		t.Errorf("MaxConnIdleTime = %v, want the connector's %v",
			poolConfig.MaxConnIdleTime, awsdsql.DefaultMaxConnIdleTime)
	}

	// The hook appears only for a URL that carries a search_path.
	if poolConfig.BeforeConnect != nil {
		t.Error("BeforeConnect is set with no search_path in the URL")
	}
}

func TestQuoteIdentifier(t *testing.T) {
	tests := []struct {
		in, want string
	}{
		{"users", `"users"`},
		{"user table", `"user table"`},
		{`we"ird`, `"we""ird"`},
		{"trunc\x00ated", `"trunc"`},
		{"", `""`},
	}

	for _, test := range tests {
		if got := quoteIdentifier(test.in); got != test.want {
			t.Errorf("quoteIdentifier(%q) = %s, want %s", test.in, got, test.want)
		}
	}
}

func TestQualifiedTable(t *testing.T) {
	d := &DSQL{config: &Config{SchemaName: testSchema}}
	if got, want := d.qualifiedTable("schema_migrations"), `"public"."schema_migrations"`; got != want {
		t.Errorf("qualifiedTable = %s, want %s", got, want)
	}

	// Qualifying is what covers WithInstance, whose caller supplies the pool: search_path
	// is applied by Open only. x-migrations-schema overrides whatever the connection
	// resolves to, and places this driver's own two tables.
	nonAdmin := &DSQL{config: mustParseConfig(t, "dsql://app_user@host/postgres?x-migrations-schema=app")}
	if got, want := nonAdmin.qualifiedTable("schema_migrations"), `"app"."schema_migrations"`; got != want {
		t.Errorf("qualifiedTable with x-migrations-schema = %s, want %s", got, want)
	}
}

// Every name that decides which lock a migrator is contending for has to be part of the
// key, or two migrators can hold what they each believe is the only lock.
func TestLockID(t *testing.T) {
	d := &DSQL{config: &Config{
		DatabaseName:    testDatabase,
		SchemaName:      testSchema,
		MigrationsTable: DefaultMigrationsTable,
		LockTable:       DefaultLockTable,
	}}

	first, err := d.lockID()
	if err != nil {
		t.Fatalf("lockID: %v", err)
	}
	if first == "" {
		t.Fatal("lockID is empty")
	}

	second, err := d.lockID()
	if err != nil {
		t.Fatalf("lockID second call: %v", err)
	}
	if first != second {
		t.Errorf("lockID is not stable: %q then %q", first, second)
	}

	// Each name is load-bearing on its own. The lock table especially: two migrators
	// agreeing on schema and migrations table but writing to different lock tables would
	// otherwise share a key and exclude nobody.
	for _, other := range []struct {
		name   string
		config *Config
	}{
		{"schema", &Config{DatabaseName: testDatabase, SchemaName: "elsewhere", MigrationsTable: DefaultMigrationsTable, LockTable: DefaultLockTable}},
		{"migrations table", &Config{DatabaseName: testDatabase, SchemaName: testSchema, MigrationsTable: "versions", LockTable: DefaultLockTable}},
		{"lock table", &Config{DatabaseName: testDatabase, SchemaName: testSchema, MigrationsTable: DefaultMigrationsTable, LockTable: "mutex"}},
		{"database", &Config{DatabaseName: "another", SchemaName: testSchema, MigrationsTable: DefaultMigrationsTable, LockTable: DefaultLockTable}},
	} {
		t.Run(other.name, func(t *testing.T) {
			otherID, err := (&DSQL{config: other.config}).lockID()
			if err != nil {
				t.Fatalf("lockID: %v", err)
			}
			if otherID == first {
				t.Errorf("a different %s produced the same lock ID %q", other.name, otherID)
			}
		})
	}
}

// Taking and releasing the lock retry a 40001 whatever OCCMaxRetries says: the
// no-rows-affected arm that reports ErrLocked is only reached on a second attempt, and a
// DELETE that gives up leaves the row behind. The plain retry() beside it honors the setting,
// which is the contrast worth pinning.
func TestRetryAtLeastFloorsTheRetryCount(t *testing.T) {
	config := &Config{OCCMaxRetryDelay: time.Millisecond}
	config.setDefaults()
	if config.OCCMaxRetries != 0 {
		t.Fatalf("OCCMaxRetries = %d, want the default of 0 for this test", config.OCCMaxRetries)
	}
	d := &DSQL{config: config}
	conflict := func(attempts *int) func() error {
		return func() error {
			*attempts++
			return &pgconn.PgError{Code: pgerrcode.SerializationFailure}
		}
	}

	for _, test := range []struct {
		name    string
		retries int
	}{
		{"unlock", unlockRetries},
		{"bootstrap", bootstrapRetries},
	} {
		t.Run(test.name, func(t *testing.T) {
			attempts := 0
			if err := d.retryAtLeast(context.Background(), test.retries, conflict(&attempts)); err == nil {
				t.Fatal("retryAtLeast returned nil for a persistent conflict")
			}
			if attempts != test.retries+1 {
				t.Errorf("made %d attempts, want %d: one run plus the floor", attempts, test.retries+1)
			}
		})
	}

	statementAttempts := 0
	if err := d.retry(context.Background(), conflict(&statementAttempts)); err == nil {
		t.Fatal("retry returned nil for a persistent conflict")
	}
	if statementAttempts != 1 {
		t.Errorf("retry made %d attempts at OCCMaxRetries = 0, want 1", statementAttempts)
	}

	// A caller who asked for more than the floor keeps it.
	generous := &Config{OCCMaxRetries: unlockRetries + 5, OCCMaxRetryDelay: time.Millisecond}
	generous.setDefaults()
	attempts := 0
	if err := (&DSQL{config: generous}).retryAtLeast(context.Background(), lockRetries, conflict(&attempts)); err == nil {
		t.Fatal("retryAtLeast returned nil for a persistent conflict")
	}
	if attempts != generous.OCCMaxRetries+1 {
		t.Errorf("made %d attempts, want %d: the floor must not cap OCCMaxRetries", attempts, generous.OCCMaxRetries+1)
	}
}

// Acquisition holds its retry count at lockRetries whatever the caller asked for, which is what
// keeps it inside migrate's LockTimeout — see lockRetries. Everything else keeps the caller's
// setting, which is the split the option is for.
func TestLockRetriesIgnoreOCCMaxRetries(t *testing.T) {
	const asked = 8
	if asked <= lockRetries {
		t.Fatalf("this test needs an OCCMaxRetries above lockRetries (%d)", lockRetries)
	}
	config := &Config{OCCMaxRetries: asked, OCCMaxRetryDelay: time.Millisecond}
	config.setDefaults()
	d := &DSQL{config: config}

	conflict := func(attempts *int) func() error {
		return func() error {
			*attempts++
			return &pgconn.PgError{Code: pgerrcode.SerializationFailure}
		}
	}

	attempts := 0
	if err := d.retryAcquire(context.Background(), conflict(&attempts)); err == nil {
		t.Fatal("retryAcquire returned nil for a persistent conflict")
	}
	if attempts != lockRetries+1 {
		t.Errorf("acquisition made %d attempts at OCCMaxRetries = %d, want %d: one run plus lockRetries, whatever the caller set",
			attempts, asked, lockRetries+1)
	}

	// The same setting still reaches the paths that have no deadline racing them.
	for _, test := range []struct {
		name string
		run  func(func() error) error
	}{
		{"unlock", func(fn func() error) error {
			return d.retryAtLeast(context.Background(), unlockRetries, fn)
		}},
		{"statement", func(fn func() error) error {
			return d.retry(context.Background(), fn)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			attempts := 0
			if err := test.run(conflict(&attempts)); err == nil {
				t.Fatal("returned nil for a persistent conflict")
			}
			if attempts <= lockRetries+1 {
				t.Errorf("made %d attempts, want more than acquisition's %d: OCCMaxRetries = %d must still apply here",
					attempts, lockRetries+1, asked)
			}
		})
	}
}

// Lock races migrate's LockTimeout, so its retries have to fit inside it. Held rather than
// floored — see TestLockRetriesIgnoreOCCMaxRetries — so this bound is the whole exposure.
//
// The bound is computed from the connector's own schedule so it tracks a change to either.
func TestLockRetriesFitInsideMigrateLockTimeout(t *testing.T) {
	config := &Config{} // the worst case is the default delay, which is the largest
	config.setDefaults()
	occ := config.occConfig()

	// occretry sleeps wait+jitter before each retry, jitter being up to a quarter of wait,
	// then multiplies wait and clamps it to MaxWait.
	worst := time.Duration(0)
	wait := occ.InitialWait
	for range lockRetries {
		worst += wait + wait/4
		if wait = time.Duration(float64(wait) * occ.Multiplier); wait > occ.MaxWait {
			wait = occ.MaxWait
		}
	}

	if worst >= migrate.DefaultLockTimeout {
		t.Errorf("Lock can back off for %v, at or past migrate's %v lock timeout: a retry landing after the timeout leaves a lock row nothing releases",
			worst, migrate.DefaultLockTimeout)
	}
}

// Classification decides whether a statement is run through the query API to collect a
// job_id, so a miss either loses the wait or reports a job that was never enqueued.
func TestEnqueuesAsyncJob(t *testing.T) {
	enqueues := []string{
		`CREATE INDEX ASYNC idx ON users (email)`,
		`create index async idx on users (email)`,
		`CREATE UNIQUE INDEX ASYNC idx ON users (email)`,
		// Still async DDL, and on a first run it returns a job_id like any other. What it does on a
		// re-run is toleratesMissingJob's business.
		`CREATE INDEX ASYNC IF NOT EXISTS idx ON users (email)`,
		`CREATE UNIQUE INDEX ASYNC IF NOT EXISTS idx ON users (email)`,
		"\n\t CREATE INDEX ASYNC idx ON users (email)",
		"-- add an index\nCREATE INDEX ASYNC idx ON users (email)",
		"/* add an index */ CREATE INDEX ASYNC idx ON users (email)",
		"-- CREATE TABLE decoys (id INT)\n/* and another */\nCREATE  INDEX\n  ASYNC idx ON users (email)",
		// The second form that returns a job_id. Only this spelling does: see
		// asyncValidatePattern.
		`ALTER TABLE ASYNC users VALIDATE CONSTRAINT users_email_check`,

		// A comment between the keywords, which the server reads as whitespace. Measured against
		// a live cluster: each of these enqueues a job and returns a job_id — see
		// enqueuesAsyncJob for what missing the match costs.
		`CREATE /* build it later */ INDEX ASYNC idx ON users (email)`,
		`CREATE UNIQUE /* why */ INDEX ASYNC idx ON users (email)`,
		`CREATE UNIQUE INDEX /* why */ ASYNC idx ON users (email)`,
		`CREATE INDEX/*terse*/ASYNC idx ON users (email)`,
		"CREATE -- why\nINDEX ASYNC idx ON users (email)",
		"CREATE UNIQUE -- why\nINDEX ASYNC idx ON users (email)",
		`CREATE /* outer /* inner */ still outer */ INDEX ASYNC idx ON users (email)`,
		`ALTER /* c */ TABLE ASYNC users VALIDATE CONSTRAINT users_email_check`,
		`ALTER TABLE /* c */ ASYNC users VALIDATE CONSTRAINT users_email_check`,
		`ALTER TABLE ASYNC users VALIDATE /* c */ CONSTRAINT users_email_check`,
	}
	for _, statement := range enqueues {
		if !enqueuesAsyncJob(statement) {
			t.Errorf("enqueuesAsyncJob(%q) = false, want true: its job_id would never be waited on", statement)
		}
	}

	plain := []string{
		`CREATE TABLE users (id UUID PRIMARY KEY)`,
		`CREATE INDEX idx ON users (email)`, // no ASYNC: not accepted by DSQL, and returns no job
		`ALTER TABLE users ADD COLUMN email TEXT`,
		`ALTER TABLE users VALIDATE CONSTRAINT users_email_check`, // no ASYNC
		`DROP TABLE users`,
		`DROP INDEX idx`,
		`INSERT INTO users (id) VALUES (gen_random_uuid())`,
		trivialStatement,
		// Naming the syntax in a comment or a literal is not issuing it. Replacing comments
		// must not turn either into a match: both patterns are anchored, so a mention that
		// follows another keyword stays a mention.
		"-- CREATE INDEX ASYNC idx ON users (email)\nSELECT 1",
		`SELECT 'CREATE INDEX ASYNC'`,
		`INSERT INTO users (note) VALUES ('-- CREATE INDEX ASYNC')`,
		`INSERT INTO users (note) VALUES ('/* CREATE INDEX ASYNC */')`,
		`SELECT '/*' AS opener, 'CREATE INDEX ASYNC' AS body`,
		"",
		"   \n\t ",
		"-- just a comment",
		"/* unterminated",
		"/* unterminated /* and nested",
		`CREATE /* no ASYNC here */ INDEX idx ON users (email)`,
		`CREATE TABLE /* c */ users (id UUID PRIMARY KEY)`,
		`ALTER TABLE /* c */ users VALIDATE CONSTRAINT users_email_check`,
	}
	for _, statement := range plain {
		if enqueuesAsyncJob(statement) {
			t.Errorf("enqueuesAsyncJob(%q) = true, want false: it would be run through the query API and demand a job_id", statement)
		}
	}

	// A license or a block of commented-out old DDL in front of the statement. asyncPrefixBudget
	// counts the SQL replaceComments writes rather than the text it reads, so the keywords reach the
	// patterns however long the header is. Counting the text instead left the run behind a header
	// this size unnormalized, and the driver then ran async DDL through ExecContext and dropped the
	// job_id — see asyncPrefixBudget.
	header := strings.Repeat("-- a license line for this migration file\n", 400)
	if len(header) <= asyncPrefixBudget {
		t.Fatalf("the header is %d bytes, inside the %d-byte budget, so this case no longer exercises it",
			len(header), asyncPrefixBudget)
	}
	if !enqueuesAsyncJob(header + `CREATE INDEX ASYNC idx ON users (email)`) {
		t.Error("enqueuesAsyncJob = false for async DDL behind a comment header past the budget: its job_id would never be waited on")
	}
	if !enqueuesAsyncJob(header + `ALTER TABLE ASYNC users VALIDATE CONSTRAINT users_email_check`) {
		t.Error("enqueuesAsyncJob = false for async validation behind a comment header past the budget")
	}
	if enqueuesAsyncJob(header + trivialStatement) {
		t.Error("enqueuesAsyncJob = true for plain SQL behind a comment header past the budget")
	}
}

// A statement that returns no job_id is an error for every async form but one, so this decides
// whether a re-run of a migration carrying IF NOT EXISTS reports a failure over an index that is
// already there.
func TestToleratesMissingJob(t *testing.T) {
	tolerated := []string{
		`CREATE INDEX ASYNC IF NOT EXISTS idx ON users (email)`,
		`create unique index async if not exists idx on users (email)`,
		"CREATE INDEX ASYNC\n  IF NOT EXISTS idx ON users (email)",
		"-- add an index\nCREATE INDEX ASYNC IF NOT EXISTS idx ON users (email)",
		`CREATE INDEX /* why */ ASYNC IF NOT EXISTS idx ON users (email)`,
		`CREATE INDEX ASYNC IF /* why */ NOT EXISTS idx ON users (email)`,
	}
	for _, statement := range tolerated {
		if _, tolerated := toleratesMissingJob(statement); !tolerated {
			t.Errorf("toleratesMissingJob(%q) = false, want true: a re-run over an existing index would report a failure", statement)
		}
	}

	strict := []string{
		// No IF NOT EXISTS, so no row back is the job_id this driver would never wait on.
		`CREATE INDEX ASYNC idx ON users (email)`,
		`CREATE UNIQUE INDEX ASYNC idx ON users (email)`,
		// VALIDATE CONSTRAINT has no IF NOT EXISTS spelling at all.
		`ALTER TABLE ASYNC users VALIDATE CONSTRAINT users_email_check`,
		// IF NOT EXISTS somewhere other than after ASYNC is a different statement.
		`CREATE TABLE IF NOT EXISTS users (id UUID PRIMARY KEY)`,
		`CREATE INDEX IF NOT EXISTS idx ON users (email)`,
		`SELECT 'CREATE INDEX ASYNC IF NOT EXISTS'`,
		"",
	}
	for _, statement := range strict {
		if _, tolerated := toleratesMissingJob(statement); tolerated {
			t.Errorf("toleratesMissingJob(%q) = true, want false: a job that was never enqueued would pass as applied", statement)
		}
	}
}

// The name confirmIndexIsValid looks up has to be the one the catalog holds, the server having
// folded an unquoted identifier and unwrapped a quoted one.
func TestIndexNameFromIfNotExists(t *testing.T) {
	tests := []struct {
		statement string
		want      string
	}{
		{`CREATE INDEX ASYNC IF NOT EXISTS idx ON users (email)`, "idx"},
		{`CREATE UNIQUE INDEX ASYNC IF NOT EXISTS idx ON users (email)`, "idx"},
		// An unquoted name reaches the catalog folded.
		{`CREATE INDEX ASYNC IF NOT EXISTS MixedCase ON users (email)`, "mixedcase"},
		// A quoted one keeps its case and leaves its quotes behind.
		{`CREATE INDEX ASYNC IF NOT EXISTS "MixedCase" ON users (email)`, "MixedCase"},
		// A doubled quote stands for one quote wherever it falls, and the match still ends at the
		// quote closing the name rather than running on into the statement.
		{`CREATE INDEX ASYNC IF NOT EXISTS "idx""y" ON users (email)`, `idx"y`},
		{`CREATE INDEX ASYNC IF NOT EXISTS """x" ON users (email)`, `"x`},
		{`CREATE INDEX ASYNC IF NOT EXISTS "a""" ON users (email)`, `a"`},
		{`CREATE INDEX ASYNC IF NOT EXISTS """" ON users (email)`, `"`},
		{`CREATE INDEX ASYNC IF NOT EXISTS "a" ON users ("b""c")`, "a"},
		{`create index async if not exists idx on users (email)`, "idx"},
		{"CREATE INDEX ASYNC\n  IF NOT EXISTS idx ON users (email)", "idx"},
		{"-- add an index\nCREATE INDEX ASYNC IF NOT EXISTS idx ON users (email)", "idx"},
		{`CREATE INDEX /* why */ ASYNC IF NOT EXISTS idx ON users (email)`, "idx"},
		// Nothing to check for a statement that names no index, which leaves the no-row path
		// tolerant as it was.
		{`CREATE INDEX ASYNC idx ON users (email)`, ""},
		{`ALTER TABLE ASYNC users VALIDATE CONSTRAINT users_email_check`, ""},
		{`CREATE INDEX IF NOT EXISTS idx ON users (email)`, ""},
		{"", ""},
	}
	for _, test := range tests {
		if got := indexNameFromIfNotExists(test.statement); got != test.want {
			t.Errorf("indexNameFromIfNotExists(%q) = %q, want %q", test.statement, got, test.want)
		}
	}
}

// Copied alongside computeLineFromPos from database/pgx/v5, so the reported position keeps
// matching the server's.
func TestComputeLineFromPos(t *testing.T) {
	tests := []struct {
		statement string
		pos       int
		line, col uint
		ok        bool
	}{
		{trivialStatement, 1, 1, 1, true},
		{trivialStatement, 8, 1, 8, true},
		{"SELECT 1\nFROM nope", 10, 2, 1, true},
		{"a\r\nb", 3, 2, 1, true},
		{trivialStatement, 99, 0, 0, false},
	}

	for _, test := range tests {
		line, col, ok := computeLineFromPos(test.statement, test.pos)
		if ok != test.ok || line != test.line || col != test.col {
			t.Errorf("computeLineFromPos(%q, %d) = (%d, %d, %v), want (%d, %d, %v)",
				test.statement, test.pos, line, col, ok, test.line, test.col, test.ok)
		}
	}
}

// replaceComments feeds the classifier above, so what matters is that the keywords end up
// adjacent and that nothing outside a comment is lost.
func TestReplaceComments(t *testing.T) {
	tests := []struct {
		name      string
		statement string
		want      string
	}{
		{"no comment is returned as it stands", `CREATE INDEX ASYNC idx`, `CREATE INDEX ASYNC idx`},
		{"block comment becomes one space", `CREATE/*c*/INDEX`, `CREATE INDEX`},
		{"surrounding whitespace is kept", `CREATE /*c*/ INDEX`, `CREATE   INDEX`},
		{"line comment becomes one space", "CREATE -- c\nINDEX", "CREATE  INDEX"},
		{"nested block comment is one comment", `A /* x /* y */ z */ B`, `A   B`},
		{"unterminated block comment runs to the end", `A /* x`, `A  `},
		{"unterminated line comment runs to the end", `A -- x`, `A  `},
		{"a comment is all there is", `/* x */`, ` `},
		{"empty stays empty", ``, ``},
		{"several comments in a row", `A /*x*/ /*y*/ B`, `A     B`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := replaceComments(test.statement); got != test.want {
				t.Errorf("replaceComments(%q) = %q, want %q", test.statement, got, test.want)
			}
		})
	}

	// Past asyncPrefixBudget the text goes over untouched rather than being rewritten, so a huge
	// file costs one copy rather than a scan of all of it. The statement is still returned whole.
	long := strings.Repeat("x", asyncPrefixBudget*2)
	if got := replaceComments(`/*c*/` + long); got != ` `+long {
		t.Errorf("replaceComments dropped or rewrote text past the budget: got %d bytes, want %d", len(got), len(long)+1)
	}
}

// Drop holds the lock in a row of the lock table, so that table is dropped after every other
// object, not merely after the other tables. A DROP that fails then returns with the row still
// there for Unlock to release, rather than with the lock table gone and the row about to be
// deleted from a table some other process recreated — see Unlock.
func TestDropOrdersTheLockTableLast(t *testing.T) {
	d := &DSQL{config: &Config{
		SchemaName:      testSchema,
		MigrationsTable: DefaultMigrationsTable,
		LockTable:       DefaultLockTable,
	}}
	lockDrop := `DROP TABLE IF EXISTS "public"."schema_migrations_lock" CASCADE`
	objects := schemaObjects{
		tables:    []string{userTable, DefaultLockTable, "orders", DefaultMigrationsTable},
		sequences: []string{"order_number"},
		domains:   []string{"positive_amount"},
		functions: []string{`"total"(a integer, b text)`},
	}

	got := d.dropStatements(objects)
	if len(got) != 7 {
		t.Fatalf("dropStatements produced %d statements, want 7:\n%s", len(got), strings.Join(got, "\n"))
	}
	if got[len(got)-1] != lockDrop {
		t.Errorf("drop order is\n%s\nwant %q last", strings.Join(got, "\n"), lockDrop)
	}

	// A schema whose lock table is already gone still drops everything else, and names it in none
	// of the statements.
	objects.tables = withoutTable(objects.tables, DefaultLockTable)
	got = d.dropStatements(objects)
	if len(got) != 6 {
		t.Errorf("dropStatements produced %d statements, want 6 with the lock table absent:\n%s", len(got), strings.Join(got, "\n"))
	}
	for _, statement := range got {
		if statement == lockDrop {
			t.Errorf("dropStatements issued %q for a lock table the schema no longer has", statement)
		}
	}
}

// Drop is documented as emptying the schema, which means these five classes — see dropViewsQuery
// for why they are the whole set. The order is what the dependencies require, and DROP DOMAIN
// carries no CASCADE because DSQL refuses it there.
func TestDropStatementsOrderObjectsByKind(t *testing.T) {
	d := &DSQL{config: &Config{
		SchemaName:      testSchema,
		MigrationsTable: DefaultMigrationsTable,
		LockTable:       DefaultLockTable,
	}}

	got := d.dropStatements(schemaObjects{
		views:     []string{"active_users"},
		tables:    []string{userTable, DefaultLockTable, DefaultMigrationsTable},
		sequences: []string{"order_number"},
		domains:   []string{"positive_amount"},
		functions: []string{`"total"(a integer, b text)`},
	})

	want := []string{
		`DROP VIEW IF EXISTS "public"."active_users" CASCADE`,
		`DROP TABLE IF EXISTS "public"."users" CASCADE`,
		`DROP TABLE IF EXISTS "public"."schema_migrations" CASCADE`,
		`DROP SEQUENCE IF EXISTS "public"."order_number" CASCADE`,
		// The functions precede the domains: a function typed on a domain still depends on it, and
		// DROP DOMAIN takes no CASCADE to clear that with.
		`DROP FUNCTION IF EXISTS "public"."total"(a integer, b text) CASCADE`,
		`DROP DOMAIN IF EXISTS "public"."positive_amount"`,
		`DROP TABLE IF EXISTS "public"."schema_migrations_lock" CASCADE`,
	}
	if len(got) != len(want) {
		t.Fatalf("dropStatements produced %d statements, want %d:\n%s", len(got), len(want), strings.Join(got, "\n"))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("statement %d is %q, want %q", i, got[i], want[i])
		}
	}

	// An empty schema drops nothing rather than issuing a statement per empty class.
	if empty := d.dropStatements(schemaObjects{}); len(empty) != 0 {
		t.Errorf("dropStatements on an empty schema produced %v, want nothing", empty)
	}
}

// A domain can be built on another, and DROP DOMAIN carries no CASCADE on DSQL, so a base dropped
// ahead of the domain standing on it reports 2BP01 and ends the run.
func TestDomainsDependentsFirst(t *testing.T) {
	// price on positive_amount on an ordinary type, and the catalog returning the base first.
	bases := map[string]string{
		"positive_amount": "",
		"price":           "positive_amount",
		"currency":        "",
	}

	got := domainsDependentsFirst([]string{"positive_amount", "price", "currency"}, bases)
	want := []string{"price", "positive_amount", "currency"}
	if !slices.Equal(got, want) {
		t.Errorf("domainsDependentsFirst = %v, want %v: the base has to go after the domain built on it", got, want)
	}

	// A chain of three, read in an order that puts the deepest last.
	chain := map[string]string{"amount": "", "positive_amount": "amount", "price": "positive_amount"}
	got = domainsDependentsFirst([]string{"amount", "positive_amount", "price"}, chain)
	want = []string{"price", "positive_amount", "amount"}
	if !slices.Equal(got, want) {
		t.Errorf("domainsDependentsFirst = %v, want %v", got, want)
	}

	// Domains of equal depth keep the catalog's order, so Drop issues the same statements twice.
	flat := []string{"b", "a", "c"}
	got = domainsDependentsFirst(flat, map[string]string{"a": "", "b": "", "c": ""})
	if !slices.Equal(got, flat) {
		t.Errorf("domainsDependentsFirst = %v, want %v: equal depths reorder nothing", got, flat)
	}

	if got = domainsDependentsFirst(nil, nil); len(got) != 0 {
		t.Errorf("domainsDependentsFirst on no domains = %v, want nothing", got)
	}
}

// Drop removes the lock table, so the Unlock Migrate issues next has no row of this driver's to
// release, and deleting by lock_id alone would take the row held by a process that recreated the
// table — see Unlock.
//
// The nil *sql.DB is the assertion: a DELETE would panic on it.
func TestUnlockAfterDropIssuesNoDelete(t *testing.T) {
	d := &DSQL{config: &Config{
		SchemaName:      testSchema,
		DatabaseName:    testDatabase,
		MigrationsTable: DefaultMigrationsTable,
		LockTable:       DefaultLockTable,
	}}
	d.isLocked.Store(true)
	d.lockTableDropped.Store(true)

	if err := d.Unlock(); err != nil {
		t.Fatalf("Unlock after Drop: %v", err)
	}
	if d.isLocked.Load() {
		t.Error("Unlock left the driver locked; the state transition has to run even when the DELETE is skipped")
	}

	// Taking the lock again means this instance holds a row of its own, so the suppression lifts
	// and a later Unlock has something to release. Reaching the nil *sql.DB is what says the
	// DELETE was attempted, which keeps the check above honest: it passes because the statement
	// was skipped, not because Unlock never issues one.
	d.lockTableDropped.Store(false)
	d.isLocked.Store(true)
	if !panics(func() { _ = d.Unlock() }) {
		t.Error("Unlock issued no DELETE once the suppression was cleared")
	}
}

// panics reports whether fn panicked, which is how a test asserts that a statement reached this
// driver's nil *sql.DB.
func panics(fn func()) (panicked bool) {
	defer func() {
		if recover() != nil {
			panicked = true
		}
	}()
	fn()
	return false
}

// One table cannot serve both, and nothing downstream catches it — see validateTableNames.
func TestValidateTableNamesRejectsACollision(t *testing.T) {
	// The collision a default supplies: naming the migrations table after the lock table's
	// default, with no x-lock-table. This is why the check runs after setDefaults.
	config := mustParseConfig(t, "dsql://admin@cluster:5432/postgres?x-migrations-table="+DefaultLockTable)
	config.setDefaults()
	if err := config.validateTableNames(); err == nil {
		t.Errorf("x-migrations-table=%s with no x-lock-table was accepted; both tables are %q",
			DefaultLockTable, config.MigrationsTable)
	}

	// And the other way round.
	config = mustParseConfig(t, "dsql://admin@cluster:5432/postgres?x-lock-table="+DefaultMigrationsTable)
	config.setDefaults()
	if err := config.validateTableNames(); err == nil {
		t.Errorf("x-lock-table=%s with no x-migrations-table was accepted", DefaultMigrationsTable)
	}

	// Both named the same explicitly.
	explicit := &Config{MigrationsTable: "one_table", LockTable: "one_table"}
	explicit.setDefaults()
	if err := explicit.validateTableNames(); err == nil {
		t.Error("two tables named the same explicitly were accepted")
	}

	// The defaults, and any distinct pair, are fine.
	for _, config := range []*Config{
		{},
		{MigrationsTable: "migrations", LockTable: "migrations_lock"},
		{MigrationsTable: DefaultLockTable, LockTable: "somewhere_else"},
	} {
		config.setDefaults()
		if err := config.validateTableNames(); err != nil {
			t.Errorf("rejected a distinct pair %q/%q: %v", config.MigrationsTable, config.LockTable, err)
		}
	}
}

// pgconn.ParseConfig merges its defaults, then PG* variables, then the connection string, so a
// config parsed from an empty string leaves the environment in charge of the pool. poolDSN names
// the keys so the connection string wins. What the connector does not overwrite is what matters
// here: ConnectTimeout, a PGHOST list's fallback targets, and PGTARGETSESSIONATTRS's connect check.
func TestPoolConfigIgnoresThePGEnvironment(t *testing.T) {
	// An unparseable PGPORT fails the parse outright, before the URL's own valid port is read.
	t.Setenv("PGPORT", "notanumber")
	t.Setenv("PGHOST", "elsewhere.example,fallback.example")
	t.Setenv("PGCONNECT_TIMEOUT", "7")
	t.Setenv("PGTARGETSESSIONATTRS", "read-write")
	t.Setenv("PGDATABASE", "somewhere_else")
	// These name files, so they fail the parse rather than misconfiguring it, and they do so even
	// under sslmode=disable.
	t.Setenv("PGSSLROOTCERT", "/nonexistent-ca.pem")
	t.Setenv("PGSSLCERT", "/nonexistent-cert.pem")
	t.Setenv("PGSSLKEY", "/nonexistent-key.pem")

	poolConfig, err := poolConfigFor("")
	if err != nil {
		t.Fatalf("poolConfigFor with PG* set: %v", err)
	}

	if poolConfig.ConnConfig.ConnectTimeout != 0 {
		t.Errorf("ConnectTimeout = %v, want 0: PGCONNECT_TIMEOUT reached the pool", poolConfig.ConnConfig.ConnectTimeout)
	}
	if n := len(poolConfig.ConnConfig.Fallbacks); n != 0 {
		t.Errorf("%d fallback host(s), want 0: PGHOST reached the pool, and the connector does not overwrite them", n)
	}
	if poolConfig.ConnConfig.ValidateConnect != nil {
		t.Error("ValidateConnect is set: PGTARGETSESSIONATTRS reached the pool")
	}

	// The lifetimes the connector needs are still pinned, and a search_path still gets its hook.
	if poolConfig.MaxConnLifetime != awsdsql.DefaultMaxConnLifetime {
		t.Errorf("MaxConnLifetime = %v, want the connector's %v", poolConfig.MaxConnLifetime, awsdsql.DefaultMaxConnLifetime)
	}
	withPath, err := poolConfigFor("app")
	if err != nil {
		t.Fatalf("poolConfigFor with a search_path: %v", err)
	}
	if withPath.BeforeConnect == nil {
		t.Error("a search_path in the URL lost its BeforeConnect hook")
	}
}

// pgVariables is every variable pgconn 5.9.2 reads from the environment, each with a value that
// would show up in the pool config if it got there. Keeping the list complete is the point: the
// ones worth pinning were found by setting all of them, not by reasoning about which mattered.
var pgVariables = []struct {
	name, value string
	// runtimeParam is the RuntimeParams key this variable legitimately populates, for the three
	// that reach the map. The connector replaces RuntimeParams before connecting, and pinning them
	// empty in poolDSN would send an empty value that DSQL rejects, so reaching the config is the
	// intended outcome for these and not a leak.
	runtimeParam string
}{
	{name: "PGHOST", value: "elsewhere.example,fallback.example"},
	{name: "PGPORT", value: "6543"},
	{name: "PGDATABASE", value: "somewhere_else"},
	{name: "PGUSER", value: "someone_else"},
	{name: "PGPASSWORD", value: "hunter2"},
	// A password file that exists and matches, since one pgx cannot read leaves the config alone
	// and would let the passfile pin go untested. The value is filled in by the test.
	{name: "PGPASSFILE"},
	{name: "PGAPPNAME", value: "somebody_elses_app", runtimeParam: "application_name"},
	{name: "PGCONNECT_TIMEOUT", value: "7"},
	{name: "PGSSLMODE", value: "require"},
	{name: "PGSSLKEY", value: "/nonexistent-key.pem"},
	{name: "PGSSLCERT", value: "/nonexistent-cert.pem"},
	{name: "PGSSLSNI", value: "0"},
	{name: "PGSSLROOTCERT", value: "/nonexistent-ca.pem"},
	{name: "PGSSLPASSWORD", value: "not-the-ssl-password"},
	{name: "PGSSLNEGOTIATION", value: "direct"},
	{name: "PGTARGETSESSIONATTRS", value: "read-write"},
	{name: "PGSERVICE", value: "some-service"},
	{name: "PGSERVICEFILE", value: "/nonexistent-service.conf"},
	{name: "PGTZ", value: "Mars/Olympus", runtimeParam: "timezone"},
	{name: "PGOPTIONS", value: "-c statement_timeout=1", runtimeParam: "options"},
	{name: "PGMINPROTOCOLVERSION", value: "3.2"},
	{name: "PGMAXPROTOCOLVERSION", value: "3.2"},
}

// poolFingerprint renders the parts of a pool config a PG* variable can reach. RuntimeParams comes
// back separately because three variables land there by design.
func poolFingerprint(t *testing.T) (string, map[string]string) {
	t.Helper()

	poolConfig, err := poolConfigFor("")
	if err != nil {
		t.Fatalf("poolConfigFor: %v", err)
	}
	c := poolConfig.ConnConfig.Config
	fingerprint := fmt.Sprintf("host=%s port=%d database=%s user=%s password=%q tls=%t "+
		"sslnegotiation=%q connectTimeout=%s minProtocol=%q maxProtocol=%q channelBinding=%q "+
		"fallbacks=%d validateConnect=%t maxConnLifetime=%s maxConnIdleTime=%s",
		c.Host, c.Port, c.Database, c.User, c.Password, c.TLSConfig != nil,
		c.SSLNegotiation, c.ConnectTimeout, c.MinProtocolVersion, c.MaxProtocolVersion,
		c.ChannelBinding, len(c.Fallbacks), c.ValidateConnect != nil,
		poolConfig.MaxConnLifetime, poolConfig.MaxConnIdleTime)
	return fingerprint, maps.Clone(c.RuntimeParams)
}

// TestPoolConfigSuppressesEveryPGVariable sets each PG* variable in turn and requires the pool
// config to come back byte for byte as it does with none of them set.
//
// PGSERVICE is the one exception, and it is asserted rather than excused: pgx resolves a service
// file before connection-string settings are merged, so poolDSN cannot outrank it. Naming the key
// empty would be worse than leaving it out, since an empty value counts as present and would send
// every Open looking for a service file.
func TestPoolConfigSuppressesEveryPGVariable(t *testing.T) {
	// A password file pgx will actually read, so PGPASSFILE reaches the config when it is not
	// pinned. poolDSN's host, port, user and database are what a passfile line has to match.
	passFile := filepath.Join(t.TempDir(), "pgpass")
	if err := os.WriteFile(passFile, []byte("localhost:5432:postgres:postgres:from-the-passfile\n"), 0o600); err != nil {
		t.Fatalf("writing a password file: %v", err)
	}

	// Emptying all of them first makes the baseline independent of whatever the developer running
	// the suite happens to export: parseEnvSettings skips empty values.
	for _, v := range pgVariables {
		t.Setenv(v.name, "")
	}
	baseFingerprint, baseRuntimeParams := poolFingerprint(t)

	for _, v := range pgVariables {
		t.Run(v.name, func(t *testing.T) {
			value := v.value
			if v.name == "PGPASSFILE" {
				value = passFile
			}
			t.Setenv(v.name, value)

			if v.name == "PGSERVICE" {
				// Point at a service file that cannot exist, so the failure does not depend on
				// whether the developer has a ~/.pg_service.conf of their own.
				t.Setenv("PGSERVICEFILE", "/nonexistent-service.conf")
				if _, err := poolConfigFor(""); err == nil {
					t.Error("PGSERVICE naming an unreadable service file did not fail the parse")
				}
				return
			}

			fingerprint, runtimeParams := poolFingerprint(t)
			if fingerprint != baseFingerprint {
				t.Errorf("%s=%s reached the pool config\n got %s\nwant %s",
					v.name, value, fingerprint, baseFingerprint)
			}
			// The three that legitimately populate RuntimeParams are allowed exactly their own key.
			if v.runtimeParam != "" {
				if got, ok := runtimeParams[v.runtimeParam]; !ok || got != value {
					t.Errorf("RuntimeParams[%q] = %q (present %t), want %q: pgx stopped mapping %s there, so poolDSN may need to pin it",
						v.runtimeParam, got, ok, value, v.name)
				}
				delete(runtimeParams, v.runtimeParam)
			}
			if !maps.Equal(runtimeParams, baseRuntimeParams) {
				t.Errorf("%s=%s changed RuntimeParams to %v, want %v",
					v.name, value, runtimeParams, baseRuntimeParams)
			}
		})
	}
}

func TestIsUndefinedTable(t *testing.T) {
	if !isUndefinedTable(&pgconn.PgError{Code: pgerrcode.UndefinedTable}) {
		t.Error("undefined table not recognized")
	}
	if isUndefinedTable(&pgconn.PgError{Code: pgerrcode.UniqueViolation}) {
		t.Error("unique violation misreported as undefined table")
	}
	if isUndefinedTable(errors.New("nope")) {
		t.Error("plain error misreported as undefined table")
	}
}

func TestWithInstanceRejectsNilConfig(t *testing.T) {
	if _, err := WithInstance(nil, nil); !errors.Is(err, ErrNilConfig) {
		t.Errorf("WithInstance(nil, nil) = %v, want ErrNilConfig", err)
	}
}

// Both paths hold OCCMaxRetries to zero or more, the range occretry's attempt <= MaxRetries
// loop runs over: parseConfig checks the URL option, validate checks a Config literal.
func TestWithInstanceRejectsNegativeOCCMaxRetries(t *testing.T) {
	_, err := WithInstance(nil, &Config{OCCMaxRetries: -1})
	if err == nil {
		t.Fatal("WithInstance accepted OCCMaxRetries = -1")
	}
	if !strings.Contains(err.Error(), "OCCMaxRetries") {
		t.Errorf("error %q does not name the offending field", err)
	}
}

// Version and Unlock classify the error retry hands back, so retry returns a non-OCC error
// untouched and on the first attempt. That is what keeps "no rows yet" reading as NilVersion
// and "the table is gone" reading as released.
func TestRetryPassesNonOCCErrorsThrough(t *testing.T) {
	config := &Config{OCCMaxRetries: 3, OCCMaxRetryDelay: time.Millisecond}
	config.setDefaults()
	d := &DSQL{config: config}

	tests := []struct {
		name string
		err  error
		is   func(error) bool
	}{
		{"no rows", sql.ErrNoRows, func(err error) bool { return errors.Is(err, sql.ErrNoRows) }},
		{"undefined table", &pgconn.PgError{Code: pgerrcode.UndefinedTable}, isUndefinedTable},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			attempts := 0
			err := d.retry(context.Background(), func() error {
				attempts++
				return test.err
			})
			if !test.is(err) {
				t.Errorf("retry returned %v, which no longer classifies", err)
			}
			if attempts != 1 {
				t.Errorf("attempts = %d, want 1: a non-OCC error must not be retried", attempts)
			}
		})
	}
}

// Both failure shapes are covered: one rejected by validate, one by the nil-config check ahead of
// it. See WithInstance for why a typed nil would read as non-nil.
func TestWithInstanceReturnsANilDriverOnError(t *testing.T) {
	tests := []struct {
		name   string
		config *Config
	}{
		{"nil config", nil},
		{"invalid config", &Config{OCCMaxRetries: -1}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			driver, err := WithInstance(nil, test.config)
			if err == nil {
				t.Fatal("WithInstance succeeded, want an error")
			}
			if driver != nil {
				t.Errorf("driver = %#v, want nil: a typed nil in the interface reads as non-nil", driver)
			}
		})
	}
}

// A blank statement is skipped rather than sent, which is what makes the trailing piece of a
// semicolon-split file harmless. Asserted without a database, since reaching one would mean
// the statement was not skipped.
func TestRunStatementSkipsBlankStatements(t *testing.T) {
	d := &DSQL{config: &Config{}}
	for _, statement := range []string{"", "   ", "\n\t\n"} {
		jobID, err := d.runStatement(context.Background(), []byte(statement))
		if err != nil {
			t.Errorf("runStatement(%q) = %v, want nil: a blank statement must not be sent", statement, err)
		}
		if jobID != "" {
			t.Errorf("runStatement(%q) reported job %q", statement, jobID)
		}
	}
}

// migrationError is what adds migrate's context, and it has to run outside the retry: a
// database.Error wrapping the PgError would hide it from the connector's classifier. So the
// PgError has to survive into the message while the sentinel stays reachable.
func TestMigrationErrorReportsTheFailingLine(t *testing.T) {
	statement := []byte("CREATE TABLE users (\n  id NOT_A_TYPE\n)")
	pgErr := &pgconn.PgError{
		Code:     pgerrcode.SyntaxError,
		Message:  `type "not_a_type" does not exist`,
		Detail:   "a detail",
		Position: 27, // inside line 2
	}

	err := migrationError(statement, pgErr)

	var dbErr database.Error
	if !errors.As(err, &dbErr) {
		t.Fatalf("migrationError returned %T, want database.Error", err)
	}
	if dbErr.Line != 2 {
		t.Errorf("Line = %d, want 2", dbErr.Line)
	}
	if !strings.Contains(dbErr.Err, pgErr.Message) {
		t.Errorf("message %q does not carry the server's message", dbErr.Err)
	}
	if !strings.Contains(dbErr.Err, pgErr.Detail) {
		t.Errorf("message %q drops the server's detail", dbErr.Err)
	}
	if dbErr.OrigErr != pgErr {
		t.Error("OrigErr is not the server's error")
	}

	// A non-Postgres error still gets context rather than being dropped.
	plain := migrationError(statement, errors.New("connection reset"))
	if !errors.As(plain, &dbErr) {
		t.Fatalf("migrationError on a plain error returned %T, want database.Error", plain)
	}
	if dbErr.Err != "migration failed" {
		t.Errorf("Err = %q, want %q", dbErr.Err, "migration failed")
	}
}

// Close must tolerate a driver that never opened anything.
func TestCloseWithoutDB(t *testing.T) {
	if err := (&DSQL{}).Close(); err != nil {
		t.Errorf("Close on a zero driver = %v, want nil", err)
	}
}

// The conformance suite in database/testing cannot catch a regression here: it runs
// against a postgres container, which supports both TRUNCATE and advisory locks, and
// TestLockAndUnlock only requires the second Lock to return *some* error. So assert
// directly that the SQL this driver emits avoids what DSQL does not implement.
//
// Only string literals are inspected, so the prose explaining why these are absent does
// not trip the check.
func TestEmitsNoUnsupportedSQL(t *testing.T) {
	banned := []string{"truncate", "pg_advisory_lock", "pg_advisory_unlock", "pg_try_advisory_lock"}

	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "dsql.go", nil, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parsing dsql.go: %v", err)
	}

	ast.Inspect(file, func(node ast.Node) bool {
		lit, ok := node.(*ast.BasicLit)
		if !ok || lit.Kind != token.STRING {
			return true
		}
		value := strings.ToLower(lit.Value)
		for _, word := range banned {
			if strings.Contains(value, word) {
				t.Errorf("%s: SQL contains %q, which Aurora DSQL does not support: %s",
					fset.Position(lit.Pos()), word, lit.Value)
			}
		}
		return true
	})
}
