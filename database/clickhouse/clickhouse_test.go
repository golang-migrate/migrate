package clickhouse_test

import (
	"context"
	"database/sql"
	sqldriver "database/sql/driver"
	"fmt"
	"log"

	"github.com/golang-migrate/migrate/v4"
	"io"
	"testing"

	"github.com/dhui/dktest"

	"github.com/golang-migrate/migrate/v4/database/clickhouse"
	dt "github.com/golang-migrate/migrate/v4/database/testing"
	"github.com/golang-migrate/migrate/v4/dktesting"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

var (
	opts = dktest.Options{
		PortRequired: true, ReadyFunc: isReady}
	specs = []dktesting.ContainerSpec{
		{ImageName: "yandex/clickhouse-server:20.4", Options: opts},
		{ImageName: "yandex/clickhouse-server:20.3", Options: opts},
		{ImageName: "yandex/clickhouse-server:20.1", Options: opts},
		{ImageName: "yandex/clickhouse-server:19.17", Options: opts},
		{ImageName: "yandex/clickhouse-server:19.16", Options: opts},
	}
)

const chPort = 9000

func chConnectionString(host, port string) string {
	return fmt.Sprintf("tcp://%s:%s", host, port)
}

func isReady(ctx context.Context, c dktest.ContainerInfo) bool {
	ip, port, err := c.Port(chPort)
	if err != nil {
		log.Println(err)
		return false
	}

	db, err := sql.Open("clickhouse", chConnectionString(ip, port))
	if err != nil {
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
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.Port(chPort)
		if err != nil {
			t.Fatal(err)
		}

		addr := chConnectionString(ip, port)
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
		ip, port, err := c.Port(chPort)
		if err != nil {
			t.Fatal(err)
		}

		addr := chConnectionString(ip, port)
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
		m, err := migrate.NewWithDatabaseInstance("file://./examples/migrations", "clickhouse", d)
		if err != nil {
			t.Fatal(err)
		}
		dt.TestMigrate(t, m)
	})
}
