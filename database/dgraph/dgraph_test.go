package dgraph

import (
	"context"
	"fmt"
	"log"
	"testing"

	"github.com/dhui/dktest"
	"github.com/golang-migrate/migrate/v4"
	dt "github.com/golang-migrate/migrate/v4/database/testing"
	"github.com/golang-migrate/migrate/v4/dktesting"

	_ "github.com/golang-migrate/migrate/v4/source/file"
)

func dgraphConnectionString(host string, port string) string {
	return fmt.Sprintf("dgraph://%s:%s?graphql=true", host, port)
}

var (
	opts = dktest.Options{
		PortRequired: true, ReadyFunc: isReady,
	}
	specs = []dktesting.ContainerSpec{
		{ImageName: "dgraph/standalone:v21.03.2", Options: opts},
		{ImageName: "dgraph/standalone:v21.03.1", Options: opts},
		{ImageName: "dgraph/standalone:v20.11.3", Options: opts},
	}
)

func isReady(ctx context.Context, c dktest.ContainerInfo) bool {
	ip, port, err := c.Port(9080)
	if err != nil {
		return false
	}
	addr := dgraphConnectionString(ip, port)
	p := &DGraph{}
	d, err := p.Open(addr)
	if err != nil {
		return false
	}
	defer func() {
		if err := d.Close(); err != nil {
			log.Println("close error:", err)
		}
	}()
	return true
}

func Test(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.Port(9080)
		if err != nil {
			t.Fatal(err)
		}

		addr := dgraphConnectionString(ip, port)
		p := &DGraph{}
		d, err := p.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()
		dt.TestNilVersion(t, d)
		dt.TestLockAndUnlock(t, d)
		dt.TestSetVersion(t, d)
		dt.TestDrop(t, d)
	})
}

func TestMigrate(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.Port(9080)
		if err != nil {
			t.Fatal(err)
		}

		addr := dgraphConnectionString(ip, port)
		p := &DGraph{}
		d, err := p.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()
		m, err := migrate.NewWithDatabaseInstance("file://./examples/migrations", "", d)
		if err != nil {
			t.Fatal(err)
		}
		dt.TestMigrate(t, m)
	})
}
