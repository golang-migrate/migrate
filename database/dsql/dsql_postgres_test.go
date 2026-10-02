package dsql

// Aurora DSQL has no local, Docker or Testcontainers image, so these tests drive the driver
// against real PostgreSQL through WithInstance — the same stand-in database/redshift uses for
// Redshift. Every statement this driver issues is plain PostgreSQL, so a real server covers
// the lock protocol, the version bookkeeping and Drop, which otherwise reach a database only
// in dsql_integration_test.go and so only when a cluster is configured.
//
// PostgreSQL stands in for the protocol, not for compatibility: it accepts SQL that DSQL
// refuses, so TestEmitsNoUnsupportedSQL and the live-cluster suite stay what guard that
// direction. sys.jobs and CREATE INDEX ASYNC have no PostgreSQL equivalent, so awaitAsyncJob
// and runStatement's asynchronous branch remain live-cluster only.
//
// The image is postgres:14 and newer, as the postgres and pgx/v5 drivers use: the lock
// protocol needs INSERT ... ON CONFLICT, which arrived in PostgreSQL 9.5.

import (
	"context"
	"database/sql"
	sqldriver "database/sql/driver"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/dhui/dktest"
	"github.com/golang-migrate/migrate/v4/database"
	dt "github.com/golang-migrate/migrate/v4/database/testing"
	"github.com/golang-migrate/migrate/v4/dktesting"
)

const pgPassword = "postgres"

var (
	pgOpts = dktest.Options{
		Env:          map[string]string{"POSTGRES_PASSWORD": pgPassword},
		PortRequired: true, ReadyFunc: pgIsReady,
	}
	pgSpecs = []dktesting.ContainerSpec{
		{ImageName: "postgres:14", Options: pgOpts},
		{ImageName: "postgres:18", Options: pgOpts},
	}
)

func pgConnectionString(host, port string) string {
	return fmt.Sprintf("postgres://postgres:%s@%s:%s/postgres?sslmode=disable", pgPassword, host, port)
}

func pgIsReady(ctx context.Context, c dktest.ContainerInfo) bool {
	ip, port, err := c.FirstPort()
	if err != nil {
		return false
	}
	// "pgx" is registered by the stdlib import in dsql.go, so no extra driver is needed here.
	db, err := sql.Open("pgx", pgConnectionString(ip, port))
	if err != nil {
		return false
	}
	defer func() {
		if err := db.Close(); err != nil {
			log.Println("close error:", err)
		}
	}()
	if err := db.PingContext(ctx); err != nil {
		switch err {
		case sqldriver.ErrBadConn, io.EOF:
		default:
			log.Println(err)
		}
		return false
	}
	return true
}

// openPostgres builds a driver over the container, with its own table names so each subtest
// keeps its own lock and version rows.
func openPostgres(t *testing.T, c dktest.ContainerInfo, config *Config) (*sql.DB, *DSQL) {
	t.Helper()

	ip, port, err := c.FirstPort()
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("pgx", pgConnectionString(ip, port))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error("closing the connection:", err)
		}
	})

	driver, err := WithInstance(db, config)
	if err != nil {
		t.Fatal("WithInstance:", err)
	}
	return db, driver.(*DSQL)
}

// requireDocker skips when no daemon is reachable, so this tier stays optional for a
// contributor working without one. The unit tests cover the driver's logic on their own, and
// CI has a daemon, so nothing here goes unexercised on a PR. dktest reports a missing daemon
// as a failure, hence the probe ahead of it.
//
// A DOCKER_HOST that is set is left to dktest: someone who configured an endpoint wants to use
// it, and a failure naming it is more use than a silent skip.
func requireDocker(t *testing.T) {
	t.Helper()

	if os.Getenv("DOCKER_HOST") != "" {
		return
	}
	conn, err := net.DialTimeout("unix", defaultDockerSocket, 2*time.Second)
	if err != nil {
		t.Skipf("no Docker daemon on %s, so the PostgreSQL stand-in is skipped: %v", defaultDockerSocket, err)
	}
	if err := conn.Close(); err != nil {
		t.Logf("closing the probe connection: %v", err)
	}
}

const defaultDockerSocket = "/var/run/docker.sock"

func TestPostgresStandIn(t *testing.T) {
	requireDocker(t)

	t.Run("conformance", testPGConformance)
	t.Run("lockIsExclusive", testPGLockIsExclusive)
	t.Run("forceLockClearsTheRow", testPGForceLock)
	t.Run("openDoesNotForceAnotherProcessesLock", testPGOpenDoesNotForce)
	t.Run("unlockToleratesADroppedLockTable", testPGUnlockAfterDrop)
	t.Run("unlockAfterDropLeavesAnotherProcessAlone", testPGUnlockAfterDropLeavesAnotherProcessAlone)
	t.Run("multiStatement", testPGMultiStatement)
	t.Run("drop", testPGDrop)

	t.Cleanup(func() {
		for _, spec := range pgSpecs {
			if err := spec.Cleanup(); err != nil {
				t.Error("removing", spec.ImageName, "error:", err)
			}
		}
	})
}

// testPGConformance runs the shared suite, which covers NilVersion, Lock/Unlock, Run,
// SetVersion, Version and Drop in the order a migration uses them.
func testPGConformance(t *testing.T) {
	dktesting.ParallelTest(t, pgSpecs, func(t *testing.T, c dktest.ContainerInfo) {
		_, d := openPostgres(t, c, &Config{
			MigrationsTable: "conformance_version",
			LockTable:       "conformance_lock",
		})
		dt.Test(t, d, []byte("SELECT 1"))
	})
}

// testPGLockIsExclusive is what the shared suite cannot assert: its TestLockAndUnlock only
// requires some error from the second Lock, while ErrLocked is what migrate reports as
// "can't acquire lock".
func testPGLockIsExclusive(t *testing.T) {
	dktesting.ParallelTest(t, pgSpecs, func(t *testing.T, c dktest.ContainerInfo) {
		config := func() *Config {
			return &Config{MigrationsTable: "excl_version", LockTable: "excl_lock"}
		}
		// Both drivers open before either locks: opening takes the lock to create the version
		// table, as it does in cockroachdb.
		_, first := openPostgres(t, c, config())
		_, second := openPostgres(t, c, config())

		if err := first.Lock(); err != nil {
			t.Fatal("first Lock:", err)
		}
		if err := second.Lock(); !errors.Is(err, database.ErrLocked) {
			t.Errorf("second Lock = %v, want database.ErrLocked", err)
		}
		if err := first.Unlock(); err != nil {
			t.Fatal("Unlock:", err)
		}
		if err := second.Lock(); err != nil {
			t.Errorf("Lock after Unlock = %v, want it to succeed", err)
		}
		if err := second.Unlock(); err != nil {
			t.Error("Unlock:", err)
		}
	})
}

// testPGForceLock covers the break-glass path: x-force-lock releases the row a previous run
// left behind, which is the documented recovery from a migration that died holding the lock.
func testPGForceLock(t *testing.T) {
	dktesting.ParallelTest(t, pgSpecs, func(t *testing.T, c dktest.ContainerInfo) {
		config := func(force bool) *Config {
			return &Config{
				MigrationsTable: "force_version",
				LockTable:       "force_lock",
				ForceLock:       force,
			}
		}

		// All three open before anyone locks, for the reason testPGLockIsExclusive gives:
		// opening takes the lock to create the version table. Opening the forcing driver this
		// early clears nothing — Open takes the lock without forcing, and no row is there yet
		// either. See ensureVersionTable.
		_, holder := openPostgres(t, c, config(false))
		_, plain := openPostgres(t, c, config(false))
		_, forcer := openPostgres(t, c, config(true))

		if err := holder.Lock(); err != nil {
			t.Fatal("Lock:", err)
		}
		if err := plain.Lock(); !errors.Is(err, database.ErrLocked) {
			t.Fatalf("Lock without force = %v, want database.ErrLocked", err)
		}
		if err := forcer.Lock(); err != nil {
			t.Errorf("Lock with ForceLock = %v, want it to take the lock", err)
		}
		if err := forcer.Unlock(); err != nil {
			t.Error("Unlock:", err)
		}
	})
}

// testPGOpenDoesNotForce covers what keeps x-force-lock the operator's decision: the acquire Open
// makes to create the version table never forces, so a driver carrying the flag leaves a row another
// migration is holding where it is. Every command reaches Open, migrate version among them, so
// forcing there would release a live lock from a read of the state. Open carries on instead, which is
// what lets those commands run while a migration is in flight. See ensureVersionTable.
func testPGOpenDoesNotForce(t *testing.T) {
	dktesting.ParallelTest(t, pgSpecs, func(t *testing.T, c dktest.ContainerInfo) {
		config := func(force bool) *Config {
			return &Config{
				MigrationsTable: "noforce_version",
				LockTable:       "noforce_lock",
				ForceLock:       force,
			}
		}
		db, holder := openPostgres(t, c, config(false))
		if err := holder.Lock(); err != nil {
			t.Fatal("Lock:", err)
		}

		// The forcing driver opens while the lock is held, which is the sequence migrate version
		// runs on a database that is mid-migration.
		_, forcer := openPostgres(t, c, config(true))

		lockID, err := holder.lockID()
		if err != nil {
			t.Fatal("lockID:", err)
		}
		var held int
		if err := db.QueryRow(
			`SELECT count(*) FROM `+holder.qualifiedTable("noforce_lock")+` WHERE lock_id = $1`,
			lockID).Scan(&held); err != nil {
			t.Fatal(err)
		}
		if held != 1 {
			t.Error("the lock row went away when a driver carrying x-force-lock opened, so a read of the state released a running migration's lock")
		}

		// The same row is what a plain Lock still reports as held.
		_, plain := openPostgres(t, c, config(false))
		if err := plain.Lock(); !errors.Is(err, database.ErrLocked) {
			t.Errorf("Lock after a forcing Open = %v, want database.ErrLocked", err)
		}

		// x-force-lock on the Lock the operator asked for still releases it.
		if err := forcer.Lock(); err != nil {
			t.Errorf("Lock with ForceLock = %v, want it to take the lock", err)
		}
		if err := forcer.Unlock(); err != nil {
			t.Error("Unlock:", err)
		}
	})
}

// testPGUnlockAfterDrop covers the tolerance Unlock carries deliberately: Drop removes the
// lock table, and migrate releases the lock afterwards.
func testPGUnlockAfterDrop(t *testing.T) {
	dktesting.ParallelTest(t, pgSpecs, func(t *testing.T, c dktest.ContainerInfo) {
		db, d := openPostgres(t, c, &Config{
			MigrationsTable: "unlock_version", LockTable: "unlock_lock",
		})
		if err := d.Lock(); err != nil {
			t.Fatal("Lock:", err)
		}
		if _, err := db.Exec(`DROP TABLE ` + d.qualifiedTable("unlock_lock")); err != nil {
			t.Fatal("dropping the lock table:", err)
		}
		if err := d.Unlock(); err != nil {
			t.Errorf("Unlock after the lock table went away = %v, want it to report released", err)
		}
	})
}

// testPGMultiStatement covers the x-multi-statement path: each piece gets its own statement,
// and a failure part-way stops the rest.
func testPGMultiStatement(t *testing.T) {
	dktesting.ParallelTest(t, pgSpecs, func(t *testing.T, c dktest.ContainerInfo) {
		db, d := openPostgres(t, c, &Config{
			MigrationsTable:       "multi_version",
			LockTable:             "multi_lock",
			MultiStatementEnabled: true,
			MultiStatementMaxSize: DefaultMultiStatementMaxSize,
		})

		if err := d.Run(strings.NewReader(
			`CREATE TABLE multi_a (id INT PRIMARY KEY); CREATE TABLE multi_b (id INT PRIMARY KEY);`,
		)); err != nil {
			t.Fatal("Run over two statements:", err)
		}
		for _, table := range []string{"multi_a", "multi_b"} {
			var exists bool
			if err := db.QueryRow(
				`SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = $1)`,
				table).Scan(&exists); err != nil {
				t.Fatal(err)
			}
			if !exists {
				t.Errorf("%s was not created, so the split ran only part of the file", table)
			}
		}

		// The second statement is the one that fails, so the first still applies and the
		// third never runs.
		err := d.Run(strings.NewReader(
			`CREATE TABLE multi_c (id INT PRIMARY KEY); CREATE TABLE multi_c (id INT PRIMARY KEY); CREATE TABLE multi_d (id INT PRIMARY KEY);`,
		))
		if err == nil {
			t.Fatal("Run succeeded over a duplicate CREATE TABLE")
		}
		var exists bool
		if err := db.QueryRow(
			`SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'multi_d')`,
		).Scan(&exists); err != nil {
			t.Fatal(err)
		}
		if exists {
			t.Error("multi_d was created, so the split carried on past a failing statement")
		}
	})
}

// testPGDrop covers what the shared suite checks only in passing: Drop removes every table in
// the schema, and the lock table goes last so the lock covers the whole loop.
func testPGDrop(t *testing.T) {
	dktesting.ParallelTest(t, pgSpecs, func(t *testing.T, c dktest.ContainerInfo) {
		db, d := openPostgres(t, c, &Config{
			MigrationsTable: "drop_version", LockTable: "drop_lock",
		})
		if err := d.Run(strings.NewReader(`CREATE TABLE drop_users (id INT PRIMARY KEY)`)); err != nil {
			t.Fatal("Run:", err)
		}
		// One of every non-table class DSQL accepts a CREATE for, each standing alone so no
		// table's CASCADE takes it: a view over no table, a sequence owned by nothing, a domain,
		// and a LANGUAGE sql function with arguments, which needs its identity arguments to be
		// named in a DROP. All four exist in plain PostgreSQL as they do on DSQL, so the stand-in
		// covers them.
		for _, statement := range []string{
			`CREATE VIEW drop_standalone_view AS SELECT 1 AS one`,
			`CREATE SEQUENCE drop_standalone_sequence`,
			`CREATE DOMAIN drop_positive AS INT CHECK (VALUE > 0)`,
			// A domain on a domain, which fixes the order within the domains: PostgreSQL reports
			// 2BP01 for a base dropped first exactly as DSQL does, and DROP DOMAIN carries no
			// CASCADE here. See schemaDomainNames.
			`CREATE DOMAIN drop_price AS drop_positive CHECK (VALUE < 1000000)`,
			`CREATE FUNCTION drop_total(a INT, b INT) RETURNS INT AS $$ SELECT a + b $$ LANGUAGE sql`,
		} {
			if _, err := db.Exec(statement); err != nil {
				t.Fatal(statement, err)
			}
		}

		if err := d.Drop(); err != nil {
			t.Fatal("Drop:", err)
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
			if err := db.QueryRow(remaining.query, d.config.SchemaName).Scan(&count); err != nil {
				t.Fatal(err)
			}
			if count != 0 {
				t.Errorf("%d %s(s) left after Drop, want none", count, remaining.kind)
			}
		}
	})
}

// Drop takes the lock table with it, so the Unlock that Migrate issues next has no row of this
// driver's to release, and deleting by lock_id alone would take the row held by a process that
// recreated the table. See Unlock.
func testPGUnlockAfterDropLeavesAnotherProcessAlone(t *testing.T) {
	dktesting.ParallelTest(t, pgSpecs, func(t *testing.T, c dktest.ContainerInfo) {
		db, d := openPostgres(t, c, &Config{
			MigrationsTable: "foreign_version", LockTable: "foreign_lock",
		})
		if err := d.Lock(); err != nil {
			t.Fatal("Lock:", err)
		}
		if err := d.Drop(); err != nil {
			t.Fatal("Drop:", err)
		}

		// Another migrator, in the window between Drop committing and Migrate calling Unlock.
		lockID, err := d.lockID()
		if err != nil {
			t.Fatal("lockID:", err)
		}
		lockTable := d.qualifiedTable("foreign_lock")
		if _, err := db.Exec(`CREATE TABLE ` + lockTable + ` (lock_id TEXT NOT NULL PRIMARY KEY)`); err != nil {
			t.Fatal("recreating the lock table:", err)
		}
		if _, err := db.Exec(`INSERT INTO `+lockTable+` (lock_id) VALUES ($1)`, lockID); err != nil {
			t.Fatal("the other migrator taking the lock:", err)
		}

		if err := d.Unlock(); err != nil {
			t.Fatal("Unlock after Drop:", err)
		}

		var held int
		if err := db.QueryRow(`SELECT count(*) FROM `+lockTable+` WHERE lock_id = $1`, lockID).Scan(&held); err != nil {
			t.Fatal(err)
		}
		if held != 1 {
			t.Error("Unlock after Drop deleted the lock row the other migrator holds")
		}
	})
}
