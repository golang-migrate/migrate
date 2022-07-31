package cockroachdb

// error codes https://github.com/lib/pq/blob/master/error.go

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/golang-migrate/migrate/v4"
	"log"
	"strings"
	"testing"
)

import (
	"github.com/dhui/dktest"
	_ "github.com/lib/pq"
)

import (
	dt "github.com/golang-migrate/migrate/v4/database/testing"
	"github.com/golang-migrate/migrate/v4/dktesting"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

const defaultPort = 26257

var (
	opts = dktest.Options{Cmd: []string{"start", "--insecure"}, PortRequired: true, ReadyFunc: isReady}
	// Released versions: https://www.cockroachlabs.com/docs/releases/
	specs = []dktesting.ContainerSpec{
		{ImageName: "cockroachdb/cockroach:v1.0.7", Options: opts},
		{ImageName: "cockroachdb/cockroach:v1.1.9", Options: opts},
		{ImageName: "cockroachdb/cockroach:v2.0.7", Options: opts},
		{ImageName: "cockroachdb/cockroach:v2.1.3", Options: opts},
	}
)

func isReady(ctx context.Context, c dktest.ContainerInfo) bool {
	ip, port, err := c.Port(defaultPort)
	if err != nil {
		log.Println("port error:", err)
		return false
	}

	db, err := sql.Open("postgres", fmt.Sprintf("postgres://root@%v:%v?sslmode=disable", ip, port))
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

	db, err := sql.Open("postgres", fmt.Sprintf("postgres://root@%v:%v?sslmode=disable", ip, port))
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

func Test(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, ci dktest.ContainerInfo) {
		createDB(t, ci)

		ip, port, err := ci.Port(26257)
		if err != nil {
			t.Fatal(err)
		}

		addr := fmt.Sprintf("cockroach://root@%v:%v/migrate?sslmode=disable", ip, port)
		c := &CockroachDb{}
		d, err := c.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		dt.Test(t, d, []byte("SELECT 1"))
	})
}

func TestMigrate(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, ci dktest.ContainerInfo) {
		createDB(t, ci)

		ip, port, err := ci.Port(26257)
		if err != nil {
			t.Fatal(err)
		}

		addr := fmt.Sprintf("cockroach://root@%v:%v/migrate?sslmode=disable", ip, port)
		c := &CockroachDb{}
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

func TestMultiStatement(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, ci dktest.ContainerInfo) {
		createDB(t, ci)

		ip, port, err := ci.Port(26257)
		if err != nil {
			t.Fatal(err)
		}

		addr := fmt.Sprintf("cockroach://root@%v:%v/migrate?sslmode=disable", ip, port)
		c := &CockroachDb{}
		d, err := c.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		if err := d.Run(strings.NewReader("CREATE TABLE foo (foo text); CREATE TABLE bar (bar text);")); err != nil {
			t.Fatalf("expected err to be nil, got %v", err)
		}

		// make sure second table exists
		var exists bool
		if err := d.(*CockroachDb).db.QueryRow("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'bar' AND table_schema = (SELECT current_schema()))").Scan(&exists); err != nil {
			t.Fatal(err)
		}
		if !exists {
			t.Fatalf("expected table bar to exist")
		}
	})
}

func TestFilterCustomQuery(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, ci dktest.ContainerInfo) {
		createDB(t, ci)

		ip, port, err := ci.Port(26257)
		if err != nil {
			t.Fatal(err)
		}

		addr := fmt.Sprintf("cockroach://root@%v:%v/migrate?sslmode=disable&x-custom=foobar", ip, port)
		c := &CockroachDb{}
		_, err = c.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
	})
}
