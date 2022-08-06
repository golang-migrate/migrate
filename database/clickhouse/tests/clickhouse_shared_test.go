package tests

import (
	"bytes"
	"context"
	"database/sql"
	sqldriver "database/sql/driver"
	"fmt"
	"github.com/stretchr/testify/require"
	"log"
	"testing"

	"github.com/dhui/dktest"
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/clickhouse"
	dt "github.com/golang-migrate/migrate/v4/database/testing"
	"github.com/golang-migrate/migrate/v4/dktesting"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

const defaultPort = 9000

var (
	tableEngines           = []string{"TinyLog", "MergeTree"}
	databaseNameVariations = []string{"default", "db", "needs-escaping-to-work"}
	opts                   = dktest.Options{
		PortRequired: true, ReadyFunc: isReady,
	}
	specs = []dktesting.ContainerSpec{
		{ImageName: "yandex/clickhouse-server:21.3", Options: opts},
		{ImageName: "clickhouse/clickhouse-server:22.4", Options: opts},
	}
)

func runSharedTestCases(t *testing.T) {
	for _, engine := range tableEngines {
		for _, databaseName := range databaseNameVariations {
			t.Run("Test_"+engine, func(t *testing.T) { testSimple(t, databaseName, engine) })
			t.Run("Migrate_"+engine, func(t *testing.T) { testMigrate(t, databaseName, engine) })
			t.Run("Version_"+engine, func(t *testing.T) { testVersion(t, databaseName, engine) })
			t.Run("Drop_"+engine, func(t *testing.T) { testDrop(t, databaseName, engine) })
			t.Run("DatabaseIsolation_"+engine, func(t *testing.T) { testDatabaseIsolation(t, databaseName, engine) })
		}
	}
	t.Run("WithInstanceDefaultConfigValues", func(t *testing.T) {
		testSimpleWithInstanceDefaultConfigValues(t)
	})
	t.Run("WithInstanceValidationOfTargetDB", func(t *testing.T) {
		testWithInstanceValidationOfTargetDB(t)
	})
	t.Run("OpenDSNValidationOfTargetDB", func(t *testing.T) {
		testOpenDSNValidationOfTargetDB(t)
	})
}

func testSimple(t *testing.T, dbName string, engine string) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.Port(defaultPort)
		if err != nil {
			t.Fatal(err)
		}

		// Precondition: database exists before the simple test sequence
		require.NoError(t, createDatabase(ip, port, dbName))

		addr := dsnStringWithExtendedOptions(ip, port, dbName, engine)
		p := &clickhouse.ClickHouse{}
		d, err := p.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()

		dt.Test(t, d, []byte("SELECT 1"))
	})
}

func testDatabaseIsolation(t *testing.T, dbName string, engine string) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.Port(defaultPort)
		if err != nil {
			t.Fatal(err)
		}

		// Precondition: database exists before we verify the isolation of migrations
		require.NoError(t, createDatabase(ip, port, dbName))

		addr := dsnStringWithExtendedOptions(ip, port, dbName, engine)
		p := &clickhouse.ClickHouse{}
		d, err := p.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()

		// Verify that both the schema migration metadata table and the migration-generated table
		// are found on the specified database.
		beforeTables, err := getTables(t, ip, port, dbName)
		require.NoError(t, err)
		require.Equal(t, beforeTables, []string{"schema_migrations"})

		dt.TestRun(t, d, bytes.NewReader(
			[]byte("CREATE TABLE some_new_table (date Date, foobar String) ENGINE = MergeTree ORDER BY date;"),
		))

		// Verify that the schema migration table and the migration table are found in
		// the target database.
		afterTables, err := getTables(t, ip, port, dbName)
		require.NoError(t, err)
		require.Equal(t, afterTables, []string{"schema_migrations", "some_new_table"})

		// ... And not in the default database (unless that was the target database!).
		if dbName != "default" {
			defaultTables, err := getTables(t, ip, port, "default")
			require.NoError(t, err)
			require.Empty(t, defaultTables)
		}
	})
}

func testSimpleWithInstanceDefaultConfigValues(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.Port(defaultPort)
		if err != nil {
			t.Fatal(err)
		}

		addr := rawDsnString(ip, port, "")
		conn, err := sql.Open("clickhouse", addr)
		if err != nil {
			t.Fatal(err)
		}
		d, err := clickhouse.WithInstance(conn, &clickhouse.Config{})
		if err != nil {
			_ = conn.Close()
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()

		dt.Test(t, d, []byte("SELECT 1"))
	})
}

func testWithInstanceValidationOfTargetDB(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.Port(defaultPort)
		if err != nil {
			t.Fatal(err)
		}

		// Create the two databases that will be referenced inconsistently.
		require.NoError(t, createDatabase(ip, port, "dsn_specified_db"))
		require.NoError(t, createDatabase(ip, port, "config_specified_db"))

		// Create a connection which is based on a DSN specifying a DB that doesn't match the below
		addr := dsnStringWithExtendedOptions(ip, port, "dsn_specified_db", "MergeTree")
		conn, err := sql.Open("clickhouse", addr)
		require.NoError(t, err)

		// Call WithInstance using the above existing connection, using a config
		// that does not match the configured DB. If we did not fail for this reason,
		// we would see any migrations run affecting the connection-specified database
		// and the schema migrations table otherwise existing in the config specified DB.
		d, err := clickhouse.WithInstance(conn, &clickhouse.Config{
			DatabaseName: "config_specified_db",
		})
		require.Error(t, err)
		require.Nil(t, d)
		require.NoError(t, conn.Close())
	})
}

func testMigrate(t *testing.T, dbName string, engine string) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.Port(defaultPort)
		if err != nil {
			t.Fatal(err)
		}

		// Precondition: database exists before migrations are applied.
		require.NoError(t, createDatabase(ip, port, dbName))

		addr := dsnStringWithExtendedOptions(ip, port, dbName, engine)
		p := &clickhouse.ClickHouse{}
		d, err := p.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()
		m, err := migrate.NewWithDatabaseInstance("file://../examples/migrations", dbName, d)
		if err != nil {
			t.Fatal(err)
		}
		dt.TestMigrate(t, m)
	})
}

func testVersion(t *testing.T, dbName string, engine string) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		expectedVersion := 1

		ip, port, err := c.Port(defaultPort)
		if err != nil {
			t.Fatal(err)
		}

		// Precondition: database exists before the version functionality is tested
		require.NoError(t, createDatabase(ip, port, dbName))

		addr := dsnStringWithExtendedOptions(ip, port, dbName, engine)
		p := &clickhouse.ClickHouse{}
		d, err := p.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()

		err = d.SetVersion(expectedVersion, false)
		if err != nil {
			t.Fatal(err)
		}

		version, _, err := d.Version()
		if err != nil {
			t.Fatal(err)
		}

		if version != expectedVersion {
			t.Fatal("Version mismatch")
		}
	})
}

func testDrop(t *testing.T, dbName string, engine string) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.Port(defaultPort)
		if err != nil {
			t.Fatal(err)
		}

		// Precondition: database exists before the drop functionality is tested
		require.NoError(t, createDatabase(ip, port, dbName))

		addr := dsnStringWithExtendedOptions(ip, port, dbName, engine)
		p := &clickhouse.ClickHouse{}
		d, err := p.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()

		err = d.Drop()
		if err != nil {
			t.Fatal(err)
		}
	})
}

func testOpenDSNValidationOfTargetDB(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.Port(defaultPort)
		if err != nil {
			t.Fatal(err)
		}

		// Precondition: database 'db' exists before we test init validation
		require.NoError(t, createDatabase(ip, port, "db"))

		for _, tc := range []struct {
			name            string
			queryParamValue string
			pathValue       string
			expectErr       bool
		}{
			{
				name:      "neither value set",
				expectErr: false,
			},
			{
				name:      "path set to / aka default",
				pathValue: "/",
				expectErr: false,
			},
			{
				name:            "path set to / aka default and query param set",
				pathValue:       "/",
				queryParamValue: "db",
				expectErr:       false,
			},
			{
				name:      "only path value set",
				pathValue: "/db",
				expectErr: false,
			},
			{
				name:            "only query param value set",
				queryParamValue: "database=db",
				expectErr:       false,
			},
			{
				name:            "both set, matching values",
				queryParamValue: "database=db",
				pathValue:       "/db",
				expectErr:       false,
			},
			{
				name:            "both set, conflicting values",
				queryParamValue: "database=db",
				pathValue:       "/anotherdb",
				expectErr:       true,
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				testCaseDSN := fmt.Sprintf(
					"clickhouse://default@%s:%s%s?%s",
					ip,
					port,
					tc.pathValue,
					tc.queryParamValue,
				)
				ch := &clickhouse.ClickHouse{}
				_, err := ch.Open(testCaseDSN)
				if tc.expectErr {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
				}

				d, err := (&clickhouse.ClickHouse{}).Open(testCaseDSN)
				if tc.expectErr {
					require.Error(t, err)
				} else {
					if closeErr := d.Close(); closeErr != nil {
						t.Error(closeErr)
					}
					require.NoError(t, err)
				}
			})
		}
	})
}

// migrate's contract doesn't seem to include the creation of databases
// so these tests will assume the existence of the targeted database
// precedes the tested migration operations.
func createDatabase(ip, port, dbName string) error {
	// NB: database is not set in the DSN PATH here because v2 driver
	// cannot connect to a database that does not exist.
	conn, err := sql.Open("clickhouse", rawDsnString(ip, port, ""))
	if err != nil {
		return err
	}
	_, err = conn.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", dbName))
	return err
}

func getTables(t *testing.T, ip string, port string, dbName string) ([]string, error) {
	// We're not setting the DB on the connection because we specify it in the query.
	conn, err := sql.Open("clickhouse", rawDsnString(ip, port, ""))
	if err != nil {
		return nil, err
	}

	var tables []string
	rows, err := conn.Query(fmt.Sprintf("SHOW TABLES FROM `%s`", dbName))
	if err != nil {
		return []string{}, err
	}
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return []string{}, err
		}
		tables = append(tables, tableName)
	}
	require.NoError(t, rows.Close())

	return tables, nil
}

func rawDsnString(host, port, database string) string {
	return fmt.Sprintf("clickhouse://%v:%v/%v", host, port, database)
}

// dsnStringWithExtendedOptions returns a dsn string that includes reasonable
// in-test settings for the extended options supported by this migrate driver.
// NB: this string is not appropriate to pass directly to ClickHouse server,
// which seems to reject unknown settings rather than silently ignoring them.
func dsnStringWithExtendedOptions(host, port, database string, engine string) string {
	if engine != "" {
		return fmt.Sprintf(
			"%v?username=default&password=&database=%v&x-multi-statement=true&x-migrations-table-engine=%v&debug=false",
			rawDsnString(host, port, database), database, engine)
	}

	return fmt.Sprintf(
		"%v?username=default&password=&database=%v&x-multi-statement=true&debug=false",
		rawDsnString(host, port, database), database)
}

func isReady(ctx context.Context, c dktest.ContainerInfo) bool {
	ip, port, err := c.Port(defaultPort)
	if err != nil {
		return false
	}

	db, err := sql.Open("clickhouse", rawDsnString(ip, port, ""))

	if err != nil {
		log.Println("open error", err)
		return false
	}
	defer func() {
		if err := db.Close(); err != nil {
			log.Println("close error:", err)
		}
	}()

	if err = db.PingContext(ctx); err != nil {
		switch err {
		case sqldriver.ErrBadConn:
			return false
		default:
			fmt.Println(err)
		}
		return false
	}

	return true
}
