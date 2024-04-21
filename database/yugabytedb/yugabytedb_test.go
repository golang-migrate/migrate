package yugabytedb

// error codes https://github.com/lib/pq/blob/master/error.go

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/dhui/dktest"
	"github.com/golang-migrate/migrate/v4"

	_ "github.com/lib/pq"

	dt "github.com/golang-migrate/migrate/v4/database/testing"
	"github.com/golang-migrate/migrate/v4/dktesting"

	_ "github.com/golang-migrate/migrate/v4/source/file"
)

const defaultPort = 5433

var (
	opts = dktest.Options{
		Cmd:          []string{"bin/yugabyted", "start", "--daemon=false"},
		PortRequired: true,
		ReadyFunc:    isReady,
		Timeout:      time.Duration(60) * time.Second,
	}
	// Released versions: https://docs.yugabyte.com/preview/releases/release-notes/
	specs = []dktesting.ContainerSpec{
		{ImageName: "yugabytedb/yugabyte:2.14.15.0-b57", Options: opts},
		{ImageName: "yugabytedb/yugabyte:2.20.2.1-b3", Options: opts},
	}
)

func isReady(ctx context.Context, c dktest.ContainerInfo) bool {
	ip, port, err := c.Port(defaultPort)
	if err != nil {
		log.Println("port error:", err)
		return false
	}

	db, err := sql.Open("postgres", fmt.Sprintf("postgres://yugabyte:yugabyte@%v:%v?sslmode=disable", ip, port))
	if err != nil {
		log.Println("open error:", err)
		return false
	}
	if err := db.PingContext(ctx); err != nil {
		log.Println("ping error:", err)
		return false
	}
	if err := db.Close(); err != nil {
		log.Println("close error:", err)
	}
	return true
}

func createDB(t *testing.T, c dktest.ContainerInfo) {
	ip, port, err := c.Port(defaultPort)
	if err != nil {
		t.Fatal(err)
	}

	db, err := sql.Open("postgres", fmt.Sprintf("postgres://yugabyte:yugabyte@%v:%v?sslmode=disable", ip, port))
	if err != nil {
		t.Fatal(err)
	}
	if err = db.Ping(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	}()

	if _, err = db.Exec("CREATE DATABASE migrate"); err != nil {
		t.Fatal(err)
	}
}

func getConnectionString(ip, port string, options ...string) string {
	options = append(options, "sslmode=disable")

	return fmt.Sprintf("yugabyte://yugabyte:yugabyte@%v:%v/migrate?%s", ip, port, strings.Join(options, "&"))
}

func Test(t *testing.T) {
	t.Run("test", test)
	t.Run("testMigrate", testMigrate)
	t.Run("testMultiStatement", testMultiStatement)
	t.Run("testFilterCustomQuery", testFilterCustomQuery)

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
	dktesting.ParallelTest(t, specs, func(t *testing.T, ci dktest.ContainerInfo) {
		createDB(t, ci)

		ip, port, err := ci.Port(defaultPort)
		if err != nil {
			t.Fatal(err)
		}

		addr := getConnectionString(ip, port)
		c := &YugabyteDB{}
		d, err := c.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		dt.Test(t, d, []byte("SELECT 1"))
	})
}

func testMigrate(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, ci dktest.ContainerInfo) {
		createDB(t, ci)

		ip, port, err := ci.Port(defaultPort)
		if err != nil {
			t.Fatal(err)
		}

		addr := getConnectionString(ip, port)
		c := &YugabyteDB{}
		d, err := c.Open(addr)
		if err != nil {
			t.Fatal(err)
		}

		m, err := migrate.NewWithDatabaseInstance("file://./examples/migrations", "migrate", d)
		if err != nil {
			t.Fatal(err)
		}
		dt.TestMigrate(t, m)
	})
}

func testMultiStatement(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, ci dktest.ContainerInfo) {
		createDB(t, ci)

		ip, port, err := ci.Port(defaultPort)
		if err != nil {
			t.Fatal(err)
		}

		addr := getConnectionString(ip, port)
		c := &YugabyteDB{}
		d, err := c.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		if err := d.Run(strings.NewReader("CREATE TABLE foo (foo text); CREATE TABLE bar (bar text);")); err != nil {
			t.Fatalf("expected err to be nil, got %v", err)
		}

		// make sure second table exists
		var exists bool
		if err := d.(*YugabyteDB).db.QueryRow("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'bar' AND table_schema = (SELECT current_schema()))").Scan(&exists); err != nil {
			t.Fatal(err)
		}
		if !exists {
			t.Fatal("expected table bar to exist")
		}
	})
}

func testFilterCustomQuery(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, ci dktest.ContainerInfo) {
		createDB(t, ci)

		ip, port, err := ci.Port(defaultPort)
		if err != nil {
			t.Fatal(err)
		}

		addr := getConnectionString(ip, port, "x-custom=foobar")
		c := &YugabyteDB{}
		d, err := c.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		dt.Test(t, d, []byte("SELECT 1"))
	})
}
