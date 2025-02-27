package clickhouse_test

import (
	"context"
	"database/sql"
	sqldriver "database/sql/driver"
	"fmt"
	"log"
	"testing"

	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/dhui/dktest"
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/clickhouse"
	dt "github.com/golang-migrate/migrate/v4/database/testing"
	"github.com/golang-migrate/migrate/v4/dktesting"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

const defaultPort = 9000

var (
	tableEngines = []string{"TinyLog", "MergeTree"}
	opts         = dktest.Options{
		Env:          map[string]string{"CLICKHOUSE_USER": "user", "CLICKHOUSE_PASSWORD": "password", "CLICKHOUSE_DB": "db"},
		PortRequired: true, ReadyFunc: isReady,
	}
	specs = []dktesting.ContainerSpec{
		{ImageName: "clickhouse:24.8", Options: opts},
	}
)

func clickhouseConnectionString(host, port, engine string) string {
	if engine != "" {
		return fmt.Sprintf(
			"clickhouse://%v:%v?username=user&password=password&database=db&x-multi-statement=true&x-migrations-table-engine=%v&debug=false",
			host, port, engine)
	}

	return fmt.Sprintf(
		"clickhouse://%v:%v?username=user&password=password&database=db&x-multi-statement=true&debug=false",
		host, port)
}

func isReady(ctx context.Context, c dktest.ContainerInfo) bool {
	ip, port, err := c.Port(defaultPort)
	if err != nil {
		return false
	}

	db, err := sql.Open("clickhouse", clickhouseConnectionString(ip, port, ""))

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

func TestCases(t *testing.T) {
	for _, engine := range tableEngines {
		t.Run("Test_"+engine, func(t *testing.T) { testSimple(t, engine) })
		t.Run("Migrate_"+engine, func(t *testing.T) { testMigrate(t, engine) })
		t.Run("Version_"+engine, func(t *testing.T) { testVersion(t, engine) })
		t.Run("Drop_"+engine, func(t *testing.T) { testDrop(t, engine) })
	}
	t.Run("WithInstanceDefaultConfigValues", func(t *testing.T) { testSimpleWithInstanceDefaultConfigValues(t) })
}

func testSimple(t *testing.T, engine string) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.Port(defaultPort)
		if err != nil {
			t.Fatal(err)
		}

		addr := clickhouseConnectionString(ip, port, engine)
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

func testSimpleWithInstanceDefaultConfigValues(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.Port(defaultPort)
		if err != nil {
			t.Fatal(err)
		}

		addr := clickhouseConnectionString(ip, port, "")
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

func testMigrate(t *testing.T, engine string) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.Port(defaultPort)
		if err != nil {
			t.Fatal(err)
		}

		addr := clickhouseConnectionString(ip, port, engine)
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
		m, err := migrate.NewWithDatabaseInstance("file://./examples/migrations", "db", d)

		if err != nil {
			t.Fatal(err)
		}
		dt.TestMigrate(t, m)
	})
}

func testVersion(t *testing.T, engine string) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		expectedVersion := 1

		ip, port, err := c.Port(defaultPort)
		if err != nil {
			t.Fatal(err)
		}

		addr := clickhouseConnectionString(ip, port, engine)
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

func testDrop(t *testing.T, engine string) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.Port(defaultPort)
		if err != nil {
			t.Fatal(err)
		}

		addr := clickhouseConnectionString(ip, port, engine)
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
