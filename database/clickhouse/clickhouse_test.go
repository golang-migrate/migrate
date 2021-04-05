package clickhouse_test

import (
	"context"
	"database/sql"
	sqldriver "database/sql/driver"
	"fmt"
	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/dhui/dktest"
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/clickhouse"
	dt "github.com/golang-migrate/migrate/v4/database/testing"
	"github.com/golang-migrate/migrate/v4/dktesting"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"log"
	"testing"
)

const defaultPort = 9000

var (
	opts = dktest.Options{
		Env:          map[string]string{"CLICKHOUSE_USER": "user", "CLICKHOUSE_PASSWORD": "password", "CLICKHOUSE_DB": "db"},
		PortRequired: true, ReadyFunc: isReady,
	}
	specs = []dktesting.ContainerSpec{
		{ImageName: "yandex/clickhouse-server:21.3", Options: opts},
	}
)

func clickhouseConnectionString(host, port string) string {
	return fmt.Sprintf("clickhouse://%v:%v?username=user&password=password&database=db&x-multi-statement=true&debug=false", host, port)
}

func isReady(ctx context.Context, c dktest.ContainerInfo) bool {
	ip, port, err := c.Port(defaultPort)
	if err != nil {
		return false
	}

	db, err := sql.Open("clickhouse", clickhouseConnectionString(ip, port))

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

func Test(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.Port(defaultPort)
		if err != nil {
			t.Fatal(err)
		}

		addr := clickhouseConnectionString(ip, port)
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

func TestMigrate(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.Port(defaultPort)
		if err != nil {
			t.Fatal(err)
		}

		addr := clickhouseConnectionString(ip, port)
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

func TestVersion(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		expectedVersion := 1

		ip, port, err := c.Port(defaultPort)
		if err != nil {
			t.Fatal(err)
		}

		addr := clickhouseConnectionString(ip, port)
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

func TestDrop(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.Port(defaultPort)
		if err != nil {
			t.Fatal(err)
		}

		addr := clickhouseConnectionString(ip, port)
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
