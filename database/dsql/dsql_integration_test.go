package dsql

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
	dt "github.com/golang-migrate/migrate/v4/database/testing"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

// Aurora DSQL has no local, Docker or Testcontainers image, so these tests run against a real
// cluster and skip when none is configured.
//
// They are the third test tier. TestPostgresStandIn covers the lock protocol, the version
// bookkeeping and Drop against a postgres container, so asynchronous DDL and real OCC conflicts
// are what belong here, alongside TestEmitsNoUnsupportedSQL. CI skips these; dsql_test.go and
// TestPostgresStandIn gate a PR.
//
//	DSQL_CLUSTER_ENDPOINT=mycluster.dsql.us-east-1.on.aws go test ./database/dsql/...
//
// The caller needs AWS credentials on the default provider chain and dsql:DbConnectAdmin on
// the cluster.
//
// Every test runs inside one schema, DSQL_TEST_SCHEMA, and each takes its own migrations and
// lock table inside it so they do not read each other's state. The schema has to exist
// already, and it must be a scratch schema: Drop is part of what is under test, and it
// removes every table in the schema it is pointed at. A cluster allows at most 10 schemas, so
// these tests do not create one.
const (
	endpointEnvVar = "DSQL_CLUSTER_ENDPOINT"
	schemaEnvVar   = "DSQL_TEST_SCHEMA"
)

// dsn returns a connection URL for the configured cluster, skipping the test when there is
// none. Options are appended as given.
func dsn(t *testing.T, options ...string) string {
	t.Helper()

	endpoint := os.Getenv(endpointEnvVar)
	if endpoint == "" {
		t.Skipf("%s is not set; skipping the live Aurora DSQL tests", endpointEnvVar)
	}
	rawURL := "dsql://admin@" + endpoint + ":5432/postgres"
	if len(options) > 0 {
		rawURL += "?" + strings.Join(options, "&")
	}
	return rawURL
}

// scratchSchema returns the schema the live tests work in.
func scratchSchema(t *testing.T) string {
	t.Helper()

	schema := os.Getenv(schemaEnvVar)
	if schema == "" {
		t.Skipf("%s is not set; these tests drop every table in the schema they run in, so it has to be named explicitly", schemaEnvVar)
	}
	return schema
}

// testPrefix derives an identifier prefix from the test's own name, so each test's tables are
// its own and a failed run leaves something identifiable behind.
func testPrefix(t *testing.T) string {
	t.Helper()
	return strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' {
			return r
		}
		if r >= 'A' && r <= 'Z' {
			return r + ('a' - 'A')
		}
		return '_'
	}, t.Name())
}

// openScratch opens a driver on the scratch schema with tables of this test's own, and drops
// them when the test ends. The driver's own two tables are named after the test, so a test
// never reads a sibling's version row.
func openScratch(t *testing.T, options ...string) *DSQL {
	t.Helper()

	prefix := testPrefix(t)
	options = append(options,
		"x-migrations-schema="+scratchSchema(t),
		"x-migrations-table="+prefix+"_version",
		"x-lock-table="+prefix+"_lock",
	)

	driver, err := (&DSQL{}).Open(dsn(t, options...))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	d := driver.(*DSQL)
	t.Cleanup(func() {
		dropTestTables(t, d, prefix)
		if err := d.Close(); err != nil {
			t.Errorf("closing the driver: %v", err)
		}
	})
	return d
}

// dropTestTables removes what a test created, so the scratch schema does not accumulate. It reports
// rather than fails, since it runs after the test's own assertions.
//
// Every class Drop handles is covered, because Drop is under test: a test that creates one and then
// fails before its own Drop would otherwise leave it behind for the next run to trip over.
//
// Everything is filtered on the test's own prefix, unlike Drop. The scratch schema is shared between
// tests and may hold objects that belong to neither, so cleanup takes only what it can attribute.
func dropTestTables(t *testing.T, d *DSQL, prefix string) {
	t.Helper()

	ctx := context.Background()
	// Ordered as dropStatements orders them, and for the same reasons.
	for _, kind := range []struct {
		object  string
		query   string
		cascade bool
	}{
		{"VIEW", `SELECT table_name FROM information_schema.views WHERE table_schema = $1 AND table_name LIKE $2`, true},
		{"TABLE", `SELECT table_name FROM information_schema.tables WHERE table_schema = $1 AND table_type = 'BASE TABLE' AND table_name LIKE $2`, true},
		{"SEQUENCE", `SELECT sequence_name FROM information_schema.sequences WHERE sequence_schema = $1 AND sequence_name LIKE $2`, true},
		// Quoted with its identity arguments already, so it is interpolated rather than qualified.
		{"FUNCTION", `SELECT quote_ident(p.proname) || '(' || pg_get_function_identity_arguments(p.oid) || ')' ` +
			`FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = $1 AND p.proname LIKE $2`, true},
	} {
		rows, err := d.db.QueryContext(ctx, kind.query, d.config.SchemaName, prefix+"%")
		if err != nil {
			t.Logf("listing this test's %ss to clean up: %v", kind.object, err)
			continue
		}
		names := make([]string, 0)
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err == nil {
				names = append(names, name)
			}
		}
		if err := errors.Join(rows.Err(), rows.Close()); err != nil {
			t.Logf("listing this test's %ss to clean up: %v", kind.object, err)
			continue
		}
		for _, name := range names {
			target := d.qualifiedTable(name)
			if kind.object == "FUNCTION" {
				target = quoteIdentifier(d.config.SchemaName) + `.` + name
			}
			statement := `DROP ` + kind.object + ` IF EXISTS ` + target
			if kind.cascade {
				statement += ` CASCADE`
			}
			if _, err := d.db.ExecContext(ctx, statement); err != nil {
				t.Logf("cleaning up %s %s: %v", kind.object, name, err)
			}
		}
	}

	// The domains last, for the reason dropStatements has them last: DSQL refuses CASCADE on DROP
	// DOMAIN, so the functions typed on one go first.
	dropTestDomains(t, d, prefix)
}

// dropTestDomains removes this test's domains, each ahead of the domain it is built on. Two columns
// and an ordering, so it reads like schemaDomainNames rather than like the kinds above.
func dropTestDomains(t *testing.T, d *DSQL, prefix string) {
	t.Helper()

	ctx := context.Background()
	query := dropDomainsQuery + ` AND t.typname LIKE $2`
	rows, err := d.db.QueryContext(ctx, query, d.config.SchemaName, prefix+"%")
	if err != nil {
		t.Logf("listing this test's domains to clean up: %v", err)
		return
	}
	names := make([]string, 0)
	bases := make(map[string]string)
	for rows.Next() {
		var name, base string
		if err := rows.Scan(&name, &base); err == nil {
			names = append(names, name)
			bases[name] = base
		}
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		t.Logf("listing this test's domains to clean up: %v", err)
		return
	}

	for _, name := range domainsDependentsFirst(names, bases) {
		if _, err := d.db.ExecContext(ctx, `DROP DOMAIN IF EXISTS `+d.qualifiedTable(name)); err != nil {
			t.Logf("cleaning up DOMAIN %s: %v", name, err)
		}
	}
}

// sameTablesDriver opens a second driver on the same schema and the same two tables, which is
// what a competing migrator is. Each brings its own pool, so they contend through the lock
// table rather than through a shared connection.
func sameTablesDriver(t *testing.T, existing *DSQL, options ...string) *DSQL {
	t.Helper()

	driver, err := (&DSQL{}).Open(dsn(t, append(options,
		"x-migrations-schema="+existing.config.SchemaName,
		"x-migrations-table="+existing.config.MigrationsTable,
		"x-lock-table="+existing.config.LockTable,
	)...))
	if err != nil {
		t.Fatalf("Open for a competing migrator: %v", err)
	}
	t.Cleanup(func() {
		if err := driver.Close(); err != nil {
			t.Errorf("closing a competing driver: %v", err)
		}
	})
	return driver.(*DSQL)
}

func mustExec(t *testing.T, d *DSQL, statement string) {
	t.Helper()
	if _, err := d.db.ExecContext(context.Background(), statement); err != nil {
		t.Fatalf("%s: %v", statement, err)
	}
}

// TestIntegrationOpen runs the conformance suite the other drivers run, which covers
// Version, SetVersion, Run, Drop and the lock round-trip.
func TestIntegrationOpen(t *testing.T) {
	driver := openScratch(t)
	dt.Test(t, driver, []byte("SELECT 1"))
}

// TestIntegrationMigrate runs the example migrations end to end, so the version the
// migrator records is checked against a schema it actually applied.
//
// The examples use unqualified names, so search_path is what places them in the scratch
// schema rather than wherever the connection lands.
func TestIntegrationMigrate(t *testing.T) {
	driver := openScratch(t, "search_path="+scratchSchema(t))
	dt.TestMigrate(t, newMigrator(t, driver))
}

// TestIntegrationLockIsExclusive is the check the conformance suite cannot make: its
// TestLockAndUnlock only requires the second Lock to return *some* error, while what matters
// here is that it returns database.ErrLocked.
//
// Both losing shapes are covered, because DSQL adjudicates at COMMIT: a migrator that starts
// concurrently commits 40001, and one that starts after the winner committed sees the row and
// affects no rows. Retry is left at its default of 0 throughout — the forced lock retry is
// what has to carry the first shape.
func TestIntegrationLockIsExclusive(t *testing.T) {
	winner := openScratch(t)

	// Every competing driver is opened before anyone locks. Opening one takes the lock itself,
	// in ensureVersionTable, so a driver opened while another holds it fails there rather than
	// in the Lock under test. cockroachdb behaves the same way, and the postgres drivers do not
	// only because an advisory lock waits instead of reporting.
	const racers = 4
	drivers := make([]*DSQL, racers)
	for i := range drivers {
		drivers[i] = sameTablesDriver(t, winner)
	}
	late := sameTablesDriver(t, winner)
	forcer := sameTablesDriver(t, winner, "x-force-lock=true")

	// The late arrival: the winner's row is already committed and in its snapshot.
	if err := winner.Lock(); err != nil {
		t.Fatalf("the first Lock failed: %v", err)
	}
	if err := late.Lock(); !errors.Is(err, database.ErrLocked) {
		t.Errorf("a migrator arriving after the lock was taken got %v, want database.ErrLocked", err)
	}
	if err := winner.Unlock(); err != nil {
		t.Fatalf("Unlock: %v", err)
	}

	var (
		start   = make(chan struct{})
		mu      sync.Mutex
		holders int
		group   sync.WaitGroup
	)
	for _, racer := range drivers {
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			err := racer.Lock()
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				holders++
			case errors.Is(err, database.ErrLocked):
			default:
				t.Errorf("a racing Lock got %v, want nil or database.ErrLocked", err)
			}
		}()
	}
	close(start)
	group.Wait()

	if holders != 1 {
		t.Errorf("%d of %d racers believe they hold the lock, want exactly 1", holders, racers)
	}

	// Whoever holds it, x-force-lock takes it over, and the row it deleted is the only one
	// there — so the forcing migrator holds the lock afterwards.
	if err := forcer.Lock(); err != nil {
		t.Errorf("x-force-lock could not take over a held lock: %v", err)
	}
	if err := forcer.Unlock(); err != nil {
		t.Errorf("Unlock after a forced lock: %v", err)
	}
}

// TestIntegrationUnlockToleratesADroppedLockTable pins the interaction Drop relies on:
// Migrate calls Unlock after Drop has removed the lock table.
func TestIntegrationUnlockToleratesADroppedLockTable(t *testing.T) {
	driver := openScratch(t)

	if err := driver.Lock(); err != nil {
		t.Fatalf("Lock: %v", err)
	}
	if err := driver.Drop(); err != nil {
		t.Fatalf("Drop: %v", err)
	}
	if err := driver.Unlock(); err != nil {
		t.Errorf("Unlock after Drop removed the lock table = %v, want nil", err)
	}

	// And the version reads as unset rather than failing on the missing table.
	version, dirty, err := driver.Version()
	if err != nil {
		t.Fatalf("Version after Drop: %v", err)
	}
	if version != database.NilVersion || dirty {
		t.Errorf("Version after Drop = (%d, %v), want (%d, false)", version, dirty, database.NilVersion)
	}
}

// TestIntegrationOCCRetry drives concurrent writers into real OCC conflicts on the version
// table and asserts that opting in absorbs them. Eight writers contending over the single row
// SetVersion rewrites is what produces the OC000 the retry exists for; with the default of 0
// this reports "max retries (0) exceeded".
//
// Only the opted-in side is asserted. What the default does with the same load is the
// contrast, and it belongs in a test that can fail on it: the retry counts each setting
// resolves to are pinned in TestRetryAtLeastFloorsTheRetryCount, without needing a conflict to
// arise on any given run.
func TestIntegrationOCCRetry(t *testing.T) {
	const writers = 8

	driver := openScratch(t, "x-occ-max-retries=10", "x-occ-max-retry-delay=200")

	var (
		start = make(chan struct{})
		group sync.WaitGroup
		mu    sync.Mutex
		first error
	)
	for i := range writers {
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			if err := driver.SetVersion(i, false); err != nil {
				mu.Lock()
				if first == nil {
					first = err
				}
				mu.Unlock()
			}
		}()
	}
	close(start)
	group.Wait()

	if first != nil {
		t.Errorf("a concurrent SetVersion failed with retry opted in: %v", first)
	}

	// The row the writers were contending over is still the one row SetVersion maintains.
	var rows int
	query := `SELECT count(*) FROM ` + driver.qualifiedTable(driver.config.MigrationsTable)
	if err := driver.db.QueryRowContext(context.Background(), query).Scan(&rows); err != nil {
		t.Fatalf("counting version rows: %v", err)
	}
	if rows != 1 {
		t.Errorf("the version table holds %d rows after %d concurrent writers, want 1", rows, writers)
	}
}

// TestIntegrationAsyncDDL is the reason Run waits: CREATE INDEX ASYNC returns as soon as the
// build is enqueued, so without the wait migrate would clear the dirty flag over an index
// that is still building or has failed.
func TestIntegrationAsyncDDL(t *testing.T) {
	driver := openScratch(t)
	prefix := testPrefix(t)
	table := driver.qualifiedTable(prefix + "_users")

	mustExec(t, driver, `CREATE TABLE `+table+` (id INT PRIMARY KEY, email TEXT)`)
	mustExec(t, driver, `INSERT INTO `+table+` VALUES (1, 'a@example.com'), (2, 'b@example.com')`)

	// A build that succeeds: Run returns only once the index is valid.
	if err := driver.Run(strings.NewReader(
		`CREATE INDEX ASYNC ` + prefix + `_email_idx ON ` + table + ` (email)`)); err != nil {
		t.Fatalf("Run on CREATE INDEX ASYNC: %v", err)
	}
	if !indexIsValid(t, driver, prefix+"_email_idx") {
		t.Error("Run returned while the index was still building, so migrate would mark the version clean over an incomplete schema")
	}

	// A build that fails: the job reports failure with no error of its own, so Run has to
	// turn that into one. Without it the migration would be recorded as applied.
	mustExec(t, driver, `INSERT INTO `+table+` VALUES (3, 'a@example.com')`)
	err := driver.Run(strings.NewReader(
		`CREATE UNIQUE INDEX ASYNC ` + prefix + `_email_uniq ON ` + table + ` (email)`))
	if err == nil {
		t.Fatal("Run succeeded on a unique index that cannot be built over duplicate rows")
	}
	if !strings.Contains(err.Error(), "duplicate key") {
		t.Errorf("Run reported %q, which does not carry the reason the job failed", err)
	}

	// The failed build leaves an INVALID index behind that goes on enforcing uniqueness, so
	// the error has to be loud enough for an operator to go and drop it.
	if indexIsValid(t, driver, prefix+"_email_uniq") {
		t.Error("the failed index reports itself valid")
	}
	mustExec(t, driver, `DROP INDEX IF EXISTS `+driver.qualifiedTable(prefix+"_email_uniq"))
}

// TestIntegrationAsyncDDLIfNotExists runs the same index create twice. The second one succeeds on
// the cluster and returns no job_id, so there is nothing to wait on — a re-run of a migration, not
// a job that went unwaited on. See toleratesMissingJob.
func TestIntegrationAsyncDDLIfNotExists(t *testing.T) {
	driver := openScratch(t)
	prefix := testPrefix(t)
	table := driver.qualifiedTable(prefix + "_users")

	mustExec(t, driver, `CREATE TABLE `+table+` (id INT PRIMARY KEY, email TEXT)`)
	statement := `CREATE INDEX ASYNC IF NOT EXISTS ` + prefix + `_email_idx ON ` + table + ` (email)`

	for attempt := 1; attempt <= 2; attempt++ {
		if err := driver.Run(strings.NewReader(statement)); err != nil {
			t.Fatalf("Run on attempt %d of CREATE INDEX ASYNC IF NOT EXISTS: %v", attempt, err)
		}
		if !indexIsValid(t, driver, prefix+"_email_idx") {
			t.Errorf("the index is not valid after attempt %d", attempt)
		}
	}
}

// TestIntegrationAsyncDDLIfNotExistsOverInvalidIndex covers what separates the two readings of a
// skipped CREATE INDEX ASYNC IF NOT EXISTS. A failed build leaves the index in place and INVALID, so
// the re-run an operator makes after fixing the data skips and returns no job_id — the same answer
// the cluster gives for an index that works. Reporting the invalid one gives them something to act
// on: it goes on enforcing uniqueness, so a silent success would record a version whose constraint
// the schema applies and whose index it lacks. The skip over an index that works is
// TestIntegrationAsyncDDLIfNotExists. See confirmIndexIsValid.
func TestIntegrationAsyncDDLIfNotExistsOverInvalidIndex(t *testing.T) {
	driver := openScratch(t)
	prefix := testPrefix(t)
	table := driver.qualifiedTable(prefix + "_users")
	index := prefix + "_email_uniq"

	mustExec(t, driver, `CREATE TABLE `+table+` (id INT PRIMARY KEY, email TEXT)`)
	mustExec(t, driver, `INSERT INTO `+table+` VALUES (1, 'a@example.com'), (2, 'a@example.com')`)
	t.Cleanup(func() {
		mustExec(t, driver, `DROP INDEX IF EXISTS `+driver.qualifiedTable(index))
	})

	// The build the operator's first run makes, over data that has duplicates in it.
	if err := driver.Run(strings.NewReader(
		`CREATE UNIQUE INDEX ASYNC ` + index + ` ON ` + table + ` (email)`)); err == nil {
		t.Fatal("Run succeeded on a unique index over duplicate rows")
	}
	if indexIsValid(t, driver, index) {
		t.Fatal("the failed build left a valid index, so there is nothing for the re-run to find")
	}

	// The re-run, in the spelling a migration written to be idempotent carries.
	ifNotExists := `CREATE UNIQUE INDEX ASYNC IF NOT EXISTS ` + index + ` ON ` + table + ` (email)`
	err := driver.Run(strings.NewReader(ifNotExists))
	if err == nil {
		t.Fatal("Run reported the migration applied over an index left INVALID by a build that failed")
	}
	// The statement is in the error either way, so INVALID is what shows the reason reached the
	// operator rather than a bare failure they would have to diagnose.
	if !strings.Contains(err.Error(), "INVALID") {
		t.Errorf("Run reported %q, which does not say the index it found is invalid", err)
	}
	if indexIsValid(t, driver, index) {
		t.Error("the index became valid, so this run did more than skip")
	}
}

// TestIntegrationAsyncDDLWithComments covers the comment spellings the classifier has to see
// through. The engine reads a comment as whitespace, so CREATE /* later */ INDEX ASYNC enqueues a
// job and returns a job_id exactly as the plain form does, and dsql-lint accepts it too — so a
// file carrying one passes the check the README recommends. See enqueuesAsyncJob.
func TestIntegrationAsyncDDLWithComments(t *testing.T) {
	driver := openScratch(t)
	prefix := testPrefix(t)
	table := driver.qualifiedTable(prefix + "_users")

	mustExec(t, driver, `CREATE TABLE `+table+` (id INT PRIMARY KEY, email TEXT)`)
	mustExec(t, driver, `INSERT INTO `+table+` VALUES (1, 'a@example.com'), (2, 'b@example.com')`)

	for _, test := range []struct {
		name      string
		index     string
		statement func(index string) string
	}{
		{"blockCommentBeforeINDEX", prefix + "_block_idx", func(index string) string {
			return `CREATE /* build it later */ INDEX ASYNC ` + index + ` ON ` + table + ` (email)`
		}},
		{"lineCommentBeforeINDEX", prefix + "_line_idx", func(index string) string {
			return "CREATE -- build it later\nINDEX ASYNC " + index + " ON " + table + " (email)"
		}},
		{"blockCommentBeforeASYNC", prefix + "_async_idx", func(index string) string {
			return `CREATE INDEX /* asynchronously */ ASYNC ` + index + ` ON ` + table + ` (email)`
		}},
		{"nestedBlockComment", prefix + "_nested_idx", func(index string) string {
			return `CREATE /* outer /* inner */ still outer */ INDEX ASYNC ` + index + ` ON ` + table + ` (email)`
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := driver.Run(strings.NewReader(test.statement(test.index))); err != nil {
				t.Fatalf("Run: %v", err)
			}
			if !indexIsValid(t, driver, test.index) {
				t.Error("Run returned while the index was still building, so the job_id was discarded and the wait skipped")
			}
		})
	}
}

// TestIntegrationDropEmptiesTheSchema covers every class DSQL accepts a CREATE for — see
// dropViewsQuery for why these five are the whole set. Each of the four non-table
// classes stands alone here, beyond the reach of any table's CASCADE, which is what makes it
// outlive a Drop that reads base tables only.
func TestIntegrationDropEmptiesTheSchema(t *testing.T) {
	driver := openScratch(t)
	prefix := testPrefix(t)

	mustExec(t, driver, `CREATE TABLE `+driver.qualifiedTable(prefix+"_orders")+` (id INT PRIMARY KEY)`)
	mustExec(t, driver, `CREATE VIEW `+driver.qualifiedTable(prefix+"_view")+` AS SELECT 1 AS one`)
	// DSQL requires CACHE, and accepts only 1 or 65536 and above.
	mustExec(t, driver, `CREATE SEQUENCE `+driver.qualifiedTable(prefix+"_seq")+` CACHE 1`)
	mustExec(t, driver, `CREATE DOMAIN `+driver.qualifiedTable(prefix+"_positive")+` AS INT CHECK (VALUE > 0)`)
	// A domain on a domain, which is what fixes the order within the domains: the base dropped first
	// reports 2BP01, and DROP DOMAIN takes no CASCADE to clear that with. See schemaDomainNames.
	mustExec(t, driver, `CREATE DOMAIN `+driver.qualifiedTable(prefix+"_price")+
		` AS `+driver.qualifiedTable(prefix+"_positive")+` CHECK (VALUE < 1000000)`)
	// Arguments, so the DROP has to carry the identity arguments that name this overload. One of
	// them is the domain above, which is what fixes the order of the two DROPs: the domain is still
	// depended on while this function stands, and DROP DOMAIN carries no CASCADE on DSQL, so a
	// domain dropped first answers 2BP01.
	mustExec(t, driver, `CREATE FUNCTION `+driver.qualifiedTable(prefix+"_total")+
		`(a `+driver.qualifiedTable(prefix+"_positive")+`, b INT) RETURNS INT AS $$ SELECT a + b $$ LANGUAGE sql`)

	if err := driver.Drop(); err != nil {
		t.Fatalf("Drop: %v", err)
	}

	for _, remaining := range []struct {
		kind  string
		query string
	}{
		{"table", `SELECT count(*) FROM information_schema.tables WHERE table_schema = $1 AND table_type = 'BASE TABLE'`},
		{"view", `SELECT count(*) FROM information_schema.views WHERE table_schema = $1`},
		{"sequence", `SELECT count(*) FROM information_schema.sequences WHERE sequence_schema = $1`},
		{"domain", `SELECT count(*) FROM information_schema.domains WHERE domain_schema = $1`},
		{"function", `SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = $1`},
	} {
		var count int
		if err := driver.db.QueryRowContext(context.Background(), remaining.query, driver.config.SchemaName).Scan(&count); err != nil {
			t.Fatalf("counting %ss: %v", remaining.kind, err)
		}
		if count != 0 {
			t.Errorf("%d %s(s) left after Drop, want none", count, remaining.kind)
		}
	}

	// The consequence of leaving one behind: the migration that created it can be reapplied.
	mustExec(t, driver, `CREATE DOMAIN `+driver.qualifiedTable(prefix+"_positive")+` AS INT CHECK (VALUE > 0)`)
}

// TestIntegrationUnlockAfterDropLeavesAnotherProcessAlone covers the window Drop opens: it takes
// the lock table with it, and Migrate calls Unlock straight afterwards, so a competing migrator
// that recreates the table and takes the lock in between holds a row this driver would otherwise
// delete. See Unlock.
func TestIntegrationUnlockAfterDropLeavesAnotherProcessAlone(t *testing.T) {
	driver := openScratch(t)

	if err := driver.Lock(); err != nil {
		t.Fatalf("Lock: %v", err)
	}
	if err := driver.Drop(); err != nil {
		t.Fatalf("Drop: %v", err)
	}

	other := sameTablesDriver(t, driver)
	if err := other.Lock(); err != nil {
		t.Fatalf("the competing migrator taking the lock after Drop: %v", err)
	}

	if err := driver.Unlock(); err != nil {
		t.Fatalf("Unlock after Drop: %v", err)
	}

	lockID, err := driver.lockID()
	if err != nil {
		t.Fatalf("lockID: %v", err)
	}
	var held int
	if err := driver.db.QueryRowContext(context.Background(),
		`SELECT count(*) FROM `+driver.qualifiedTable(driver.config.LockTable)+` WHERE lock_id = $1`, lockID).Scan(&held); err != nil {
		t.Fatalf("counting the lock rows: %v", err)
	}
	if held != 1 {
		t.Error("Unlock after Drop deleted the lock row the competing migrator holds, leaving the lock open to a third")
	}
	if err := other.Unlock(); err != nil {
		t.Errorf("the competing migrator releasing its lock: %v", err)
	}
}

// TestIntegrationAsyncConstraintValidation covers the second statement that returns a job_id.
// It is the reason the wait cannot be special-cased to indexes: a validation that fails leaves
// the constraint unvalidated, and without the wait migrate would record the migration as
// applied.
//
// Note which spellings the engine accepts, since the pair is not symmetric: the constraint is
// added with a plain ALTER TABLE ... NOT VALID, and only the validation takes ASYNC. Adding a
// constraint with ALTER TABLE ASYNC, and validating without it, are both rejected.
func TestIntegrationAsyncConstraintValidation(t *testing.T) {
	driver := openScratch(t)
	prefix := testPrefix(t)
	table := driver.qualifiedTable(prefix + "_orders")
	constraint := prefix + "_amount_check"

	mustExec(t, driver, `CREATE TABLE `+table+` (id INT PRIMARY KEY, amount INT)`)
	mustExec(t, driver, `INSERT INTO `+table+` VALUES (1, 5), (2, 10)`)
	mustExec(t, driver, `ALTER TABLE `+table+` ADD CONSTRAINT `+constraint+` CHECK (amount > 0) NOT VALID`)

	// A validation that passes: Run returns only once the job has finished, so the constraint
	// is validated by then.
	if err := driver.Run(strings.NewReader(
		`ALTER TABLE ASYNC ` + table + ` VALIDATE CONSTRAINT ` + constraint)); err != nil {
		t.Fatalf("Run on ALTER TABLE ASYNC ... VALIDATE CONSTRAINT: %v", err)
	}
	if !constraintIsValidated(t, driver, constraint) {
		t.Error("Run returned while the constraint was still being validated, so migrate would mark the version clean over an unvalidated constraint")
	}

	// A validation that fails, on a row the constraint rejects. The job reports failure with
	// no error of its own, so Run has to turn that into one.
	violating := prefix + "_violating"
	violatingTable := driver.qualifiedTable(violating)
	mustExec(t, driver, `CREATE TABLE `+violatingTable+` (id INT PRIMARY KEY, amount INT)`)
	mustExec(t, driver, `INSERT INTO `+violatingTable+` VALUES (1, 5), (2, -3)`)
	mustExec(t, driver, `ALTER TABLE `+violatingTable+` ADD CONSTRAINT `+violating+`_chk CHECK (amount > 0) NOT VALID`)

	err := driver.Run(strings.NewReader(
		`ALTER TABLE ASYNC ` + violatingTable + ` VALIDATE CONSTRAINT ` + violating + `_chk`))
	if err == nil {
		t.Fatal("Run succeeded validating a constraint that a row violates")
	}
	if !strings.Contains(err.Error(), "violated") {
		t.Errorf("Run reported %q, which does not carry the reason the job failed", err)
	}
	if constraintIsValidated(t, driver, violating+"_chk") {
		t.Error("the failed validation reports the constraint as validated")
	}
}

// TestIntegrationSearchPath covers the pool hook: a search_path in the URL reaches every
// connection, and CURRENT_SCHEMA() resolves from it, which is what leaves
// x-migrations-schema an override rather than the only way to place the two tables.
func TestIntegrationSearchPath(t *testing.T) {
	schema := scratchSchema(t)
	prefix := testPrefix(t)

	// No x-migrations-schema: where the two tables land has to come from search_path alone.
	driver, err := (&DSQL{}).Open(dsn(t,
		"search_path="+schema,
		"x-migrations-table="+prefix+"_version",
		"x-lock-table="+prefix+"_lock",
	))
	if err != nil {
		t.Fatalf("Open with search_path: %v", err)
	}
	d := driver.(*DSQL)
	t.Cleanup(func() {
		dropTestTables(t, d, prefix)
		if err := d.Close(); err != nil {
			t.Errorf("closing the driver: %v", err)
		}
	})

	if d.config.SchemaName != schema {
		t.Errorf("CURRENT_SCHEMA() resolved to %q, want %q from search_path", d.config.SchemaName, schema)
	}

	// And the tables landed there rather than in the connection's default schema.
	var count int
	query := `SELECT count(*) FROM information_schema.tables WHERE table_schema = $1 AND table_name IN ($2, $3)`
	if err := d.db.QueryRowContext(context.Background(), query, schema, prefix+"_version", prefix+"_lock").Scan(&count); err != nil {
		t.Fatalf("counting the driver's tables: %v", err)
	}
	if count != 2 {
		t.Errorf("found %d of the driver's 2 tables in %q", count, schema)
	}
}

// TestIntegrationStatementTimeout covers x-statement-timeout, which is applied per statement
// rather than by the server.
func TestIntegrationStatementTimeout(t *testing.T) {
	driver := openScratch(t, "x-statement-timeout=1")

	err := driver.Run(strings.NewReader(`SELECT pg_sleep(5)`))
	if err == nil {
		t.Fatal("a 5s statement completed under a 1ms statement timeout")
	}
	// Matched on the message rather than with errors.Is: the deadline is inside the
	// database.Error naming the failing migration, and that type has no Unwrap. Same property
	// retry() depends on, from the other side.
	if !strings.Contains(err.Error(), context.DeadlineExceeded.Error()) {
		t.Errorf("Run reported %q, which does not say the statement ran out of time", err)
	}
}

// TestIntegrationPasswordIsRejected keeps the URL contract honest against the connector: it
// discards a password and authenticates with IAM, so accepting one would be a lie.
func TestIntegrationPasswordIsRejected(t *testing.T) {
	endpoint := os.Getenv(endpointEnvVar)
	if endpoint == "" {
		t.Skipf("%s is not set; skipping the live Aurora DSQL tests", endpointEnvVar)
	}

	for _, rawURL := range []string{
		"dsql://admin:hunter2@" + endpoint + ":5432/postgres",
		"dsql://admin@" + endpoint + ":5432/postgres?password=hunter2",
	} {
		if _, err := (&DSQL{}).Open(rawURL); !errors.Is(err, ErrPasswordSet) {
			t.Errorf("Open(%q) = %v, want ErrPasswordSet", rawURL, err)
		}
	}
}

// indexIsValid reports pg_index.indisvalid, which is false while a build is running and stays
// false after one fails.
func indexIsValid(t *testing.T, d *DSQL, indexName string) bool {
	t.Helper()
	var valid sql.NullBool
	query := `SELECT indisvalid FROM pg_index JOIN pg_class ON pg_class.oid = indexrelid WHERE relname = $1`
	if err := d.db.QueryRowContext(context.Background(), query, indexName).Scan(&valid); err != nil {
		t.Fatalf("reading the state of index %s: %v", indexName, err)
	}
	return valid.Bool
}

// constraintIsValidated reports pg_constraint.convalidated, which stays false for a constraint
// added NOT VALID until a validation job succeeds.
func constraintIsValidated(t *testing.T, d *DSQL, constraintName string) bool {
	t.Helper()
	var validated sql.NullBool
	query := `SELECT convalidated FROM pg_constraint WHERE conname = $1`
	if err := d.db.QueryRowContext(context.Background(), query, constraintName).Scan(&validated); err != nil {
		t.Fatalf("reading the state of constraint %s: %v", constraintName, err)
	}
	return validated.Bool
}

// newMigrator wires the driver to the example migrations for dt.TestMigrate.
func newMigrator(t *testing.T, d *DSQL) *migrate.Migrate {
	t.Helper()
	m, err := migrate.NewWithDatabaseInstance("file://examples/migrations", d.config.DatabaseName, d)
	if err != nil {
		t.Fatalf("NewWithDatabaseInstance: %v", err)
	}
	// The examples build an index, so give the lock and the jobs room.
	m.LockTimeout = 30 * time.Second
	return m
}
