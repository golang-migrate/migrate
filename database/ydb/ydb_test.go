package ydb

import (
	"context"
	"database/sql"
	sqldriver "database/sql/driver"
	"fmt"
	"io"
	"log"
	"strings"
	"sync"
	"testing"

	"github.com/dhui/dktest"
	"github.com/docker/go-connections/nat"
	"github.com/golang-migrate/migrate/v4"
	dt "github.com/golang-migrate/migrate/v4/database/testing"
	"github.com/golang-migrate/migrate/v4/dktesting"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	_ "github.com/ydb-platform/ydb-go-sdk/v3"
)

const defaultPort = 2136

var (
	opts = dktest.Options{
		Hostname:     "localhost",
		PortRequired: true,
		PortBindings: nat.PortMap{
			nat.Port("2135"): []nat.PortBinding{
				{
					HostPort: "2135",
				},
			},
			nat.Port("8765"): []nat.PortBinding{
				{
					HostPort: "8765",
				},
			},
			nat.Port("2136"): []nat.PortBinding{
				{
					HostPort: "2136",
				},
			},
		},
		Volumes: []string{
			"$(pwd)/ydb_certs:/ydb_certs",
			"$(pwd)/ydb_data:/ydb_data",
		},
		Env: map[string]string{
			"YDB_USE_IN_MEMORY_PDISKS": "true",
			"GRPC_TLS_PORT":            "2135",
			"GRPC_PORT":                "2136",
			"MON_PORT":                 "8765",
			"YDB_DEFAULT_LOG_LEVEL":    "NOTICE",
		},
		ReadyFunc: isReady,
	}
	specs = []dktesting.ContainerSpec{
		{ImageName: "cr.yandex/yc/yandex-docker-local-ydb:latest", Options: opts},
	}
)

func ydbConnectionString(host, port string, options ...string) string {
	// Use grpc:// (insecure) because Docker container exposes insecure gRPC on port 2136.
	// Use go_balancer=single to skip YDB SDK's node discovery which returns
	// Docker-internal hostnames that can't be resolved from the host.
	baseOpts := []string{"go_balancer=single"}
	baseOpts = append(baseOpts, options...)
	return fmt.Sprintf("grpc://%s:%s/local?%s", host, port, strings.Join(baseOpts, "&"))
}

func isReady(ctx context.Context, c dktest.ContainerInfo) bool {
	ip, port, err := c.Port(defaultPort)
	if err != nil {
		return false
	}

	db, err := sql.Open("ydb", ydbConnectionString(ip, port))
	if err != nil {
		log.Println("open error:", err)
		return false
	}
	defer func() {
		if err := db.Close(); err != nil {
			log.Println("close error:", err)
		}
	}()

	if err = db.PingContext(ctx); err != nil {
		switch err {
		case sqldriver.ErrBadConn, io.EOF:
			return false
		default:
			log.Println(err)
		}
		return false
	}

	return true
}

func Test(t *testing.T) {
	t.Run("test", test)
	t.Run("testMigrate", testMigrate)
	t.Run("testMultipleStatementsInMultiStatementMode", testMultipleStatementsInMultiStatementMode)
	t.Run("testFilterCustomQuery", testFilterCustomQuery)
	t.Run("testMigrationTableOption", testMigrationTableOption)
	t.Run("testLock", testLock)
	t.Run("testWithInstanceConcurrent", testWithInstanceConcurrent)

	t.Cleanup(func() {
		for _, spec := range specs {
			t.Log("Cleaning up ", spec.ImageName)
			if err := spec.Cleanup(); err != nil {
				t.Error("Error removing ", spec.ImageName, "error:", err)
			}
		}
	})
}

func test(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.Port(defaultPort)
		if err != nil {
			t.Fatal(err)
		}

		addr := ydbConnectionString(ip, port)
		y := &YDB{}
		d, err := y.Open(addr)
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

func testMigrate(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.Port(defaultPort)
		if err != nil {
			t.Fatal(err)
		}

		addr := ydbConnectionString(ip, port)
		y := &YDB{}
		d, err := y.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()
		m, err := migrate.NewWithDatabaseInstance("file://./examples/migrations", "ydb", d)
		if err != nil {
			t.Fatal(err)
		}
		dt.TestMigrate(t, m)
	})
}

func testMultipleStatementsInMultiStatementMode(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.Port(defaultPort)
		if err != nil {
			t.Fatal(err)
		}

		addr := ydbConnectionString(ip, port, "x-multi-statement=true")
		y := &YDB{}
		d, err := y.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()

		// Create two tables in a single multi-statement migration
		err = d.Run(strings.NewReader(
			"CREATE TABLE multi_a (id Int64 NOT NULL, PRIMARY KEY (id));" +
				"CREATE TABLE multi_b (id Int64 NOT NULL, PRIMARY KEY (id));",
		))
		if err != nil {
			t.Fatalf("expected err to be nil, got %v", err)
		}

		// Verify second table exists by inserting into it
		ydbDriver := d.(*YDB)
		if _, err := ydbDriver.db.ExecContext(context.Background(),
			"UPSERT INTO multi_b (id) VALUES (1)"); err != nil {
			t.Fatalf("expected table multi_b to exist, got error: %v", err)
		}
	})
}

func testFilterCustomQuery(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.Port(defaultPort)
		if err != nil {
			t.Fatal(err)
		}

		addr := ydbConnectionString(ip, port, "x-custom=foobar")
		y := &YDB{}
		d, err := y.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()
	})
}

func testMigrationTableOption(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.Port(defaultPort)
		if err != nil {
			t.Fatal(err)
		}

		addr := ydbConnectionString(ip, port, "x-migrations-table=custom_migrations")
		y := &YDB{}
		d, err := y.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()

		// Verify the custom migrations table is used by checking the driver config
		ydbDriver := d.(*YDB)
		if ydbDriver.config.MigrationsTable != "custom_migrations" {
			t.Fatalf("expected migrations table to be 'custom_migrations', got '%s'",
				ydbDriver.config.MigrationsTable)
		}

		// Ensure the table was actually created by setting a version
		if err := d.SetVersion(1, false); err != nil {
			t.Fatal(err)
		}
		version, dirty, err := d.Version()
		if err != nil {
			t.Fatal(err)
		}
		if version != 1 {
			t.Fatalf("expected version 1, got %d", version)
		}
		if dirty {
			t.Fatal("expected dirty to be false")
		}
	})
}

func testLock(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.Port(defaultPort)
		if err != nil {
			t.Fatal(err)
		}

		addr := ydbConnectionString(ip, port)
		y := &YDB{}
		d, err := y.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()

		ydbDriver := d.(*YDB)

		// Lock, unlock, lock, unlock cycle
		err = ydbDriver.Lock()
		if err != nil {
			t.Fatal(err)
		}

		// Locking again should fail
		err = ydbDriver.Lock()
		if err == nil {
			t.Fatal("expected error when locking twice, got nil")
		}

		err = ydbDriver.Unlock()
		if err != nil {
			t.Fatal(err)
		}

		// Should be able to lock again after unlock
		err = ydbDriver.Lock()
		if err != nil {
			t.Fatal(err)
		}

		err = ydbDriver.Unlock()
		if err != nil {
			t.Fatal(err)
		}
	})
}

func testWithInstanceConcurrent(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.Port(defaultPort)
		if err != nil {
			t.Fatal(err)
		}

		const concurrency = 30

		db, err := sql.Open("ydb", ydbConnectionString(ip, port))
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := db.Close(); err != nil {
				t.Error(err)
			}
		}()

		db.SetMaxIdleConns(concurrency)
		db.SetMaxOpenConns(concurrency)

		var wg sync.WaitGroup
		defer wg.Wait()

		wg.Add(concurrency)
		for i := 0; i < concurrency; i++ {
			go func(i int) {
				defer wg.Done()
				_, err := WithInstance(db, &Config{
					DatabaseName: "/local",
				})
				if err != nil {
					t.Errorf("process %d error: %s", i, err)
				}
			}(i)
		}
	})
}
