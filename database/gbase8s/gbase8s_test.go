package gbase8s

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"log"
	"testing"

	_ "gitee.com/GBase8s/go-gci"
	"github.com/dhui/dktest"
	"github.com/golang-migrate/migrate/v4"
	dt "github.com/golang-migrate/migrate/v4/database/testing"
	"github.com/golang-migrate/migrate/v4/dktesting"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

const (
	defaultPort = 9088
	userName    = "gbasedbt"
	userPwd     = "GBase123"
	dbName      = "testdb"
	gbaseServer = "gbase01"
)

var (
	opts = dktest.Options{
		Env:          map[string]string{"USERPASS": userPwd, "SERVERNAME": gbaseServer},
		PortRequired: true, ReadyFunc: isReady,
	}
	specs = []dktesting.ContainerSpec{
		{ImageName: "liaosnet/gbase8s:v8.8_3633x11_csdk_arm64", Options: opts},
	}
)

func gbase8sConnectionString(host, port string) string {
	return fmt.Sprintf("gbase8s://%s:%s@%s:%s/%s?GBASEDBTSERVER=%s&GCI_FACTORY=4&PROTOCOL=onsoctcp&delimident=1&sqlmode=oracle",
		userName, userPwd, host, port, dbName, gbaseServer)
}

func isReady(ctx context.Context, c dktest.ContainerInfo) bool {
	ip, port, err := c.Port(defaultPort)
	if err != nil {
		return false
	}

	db, err := sql.Open("gbase8s", gbase8sConnectionString(ip, port))
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
		case driver.ErrBadConn:
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
		p := &Gbase8s{}
		d, err := p.Open(gbase8sConnectionString(ip, port))
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
		p := &Gbase8s{}
		d, err := p.Open(gbase8sConnectionString(ip, port))
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()

		m, err := migrate.NewWithDatabaseInstance("file://./examples/migrations", "c11", d)
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
		p := &Gbase8s{}
		d, err := p.Open(gbase8sConnectionString(ip, port))
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
		p := &Gbase8s{}
		d, err := p.Open(gbase8sConnectionString(ip, port))
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

// func TestCustomQuery(t *testing.T) {
// 	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
// 		ip, port, err := c.Port(defaultPort)
// 		if err != nil {
// 			t.Fatal(err)
// 		}
// 		p := &Gbase8s{}
// 		// x-migrations-table=
// 		// x-lock-table=
// 		// x-force-lock=
// 		// x-statement-timeout
// 		_, err = p.Open(gbase8sConnectionString(ip, port) + "&x-migrations-table=mt&x-lock-table=lockt")
// 		if err != nil {
// 			t.Fatal(err)
// 		}
// 	})
// }
