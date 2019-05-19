package mssql

import (
	"context"
	"database/sql"
	sqldriver "database/sql/driver"
	"fmt"
	"log"
	"testing"

	"github.com/dhui/dktest"
	"github.com/golang-migrate/migrate/v4"

	dt "github.com/golang-migrate/migrate/v4/database/testing"
	"github.com/golang-migrate/migrate/v4/dktesting"

	_ "github.com/golang-migrate/migrate/v4/source/file"
)

const defaultPort = 1433
const saPassword = "Root1234"

var (
	opts = dktest.Options{
		Env:          map[string]string{"ACCEPT_EULA": "Y", "SA_PASSWORD": saPassword, "MSSQL_PID": "Express"},
		PortRequired: true, ReadyFunc: isReady,
	}
	// Supported versions: https://www.mysql.com/support/supportedplatforms/database.html
	specs = []dktesting.ContainerSpec{
		{ImageName: "mcr.microsoft.com/mssql/server:2017-latest-ubuntu", Options: opts},
		{ImageName: "mcr.microsoft.com/mssql/server:2019-latest", Options: opts},
	}
)

func isReady(ctx context.Context, c dktest.ContainerInfo) bool {
	ip, port, err := c.Port(defaultPort)
	if err != nil {
		return false
	}
	uri := fmt.Sprintf("sqlserver://sa:%v@%v:%v?database=master", saPassword, ip, port)
	db, err := sql.Open("sqlserver", uri)
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
	// mysql.SetLogger(mysql.Logger(log.New(ioutil.Discard, "", log.Ltime)))

	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.Port(defaultPort)
		if err != nil {
			t.Fatal(err)
		}

		addr := fmt.Sprintf("sqlserver://sa:%v@%v:%v?master", saPassword, ip, port)
		p := &MSSQL{}
		d, err := p.Open(addr)
		if err != nil {
			t.Fatalf("%v", err)
		}

		defer func() {
			if err := d.Close(); err != nil {
				log.Println("close error:", err)
			}
		}()

		dt.Test(t, d, []byte("SELECT 1"))

		// check ensureVersionTable
		if err := d.(*MSSQL).ensureVersionTable(); err != nil {
			t.Fatal(err)
		}
		// check again
		if err := d.(*MSSQL).ensureVersionTable(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestMigrate(t *testing.T) {
	// mysql.SetLogger(mysql.Logger(log.New(ioutil.Discard, "", log.Ltime)))

	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.Port(defaultPort)
		if err != nil {
			t.Fatal(err)
		}

		addr := fmt.Sprintf("sqlserver://sa:%v@%v:%v?master", saPassword, ip, port)
		p := &MSSQL{}
		d, err := p.Open(addr)
		if err != nil {
			t.Fatalf("%v", err)
		}

		defer func() {
			if err := d.Close(); err != nil {
				log.Println("close error:", err)
			}
		}()

		m, err := migrate.NewWithDatabaseInstance("file://./examples/migrations", "public", d)
		if err != nil {
			t.Fatalf("%v", err)
		}
		dt.TestMigrate(t, m, []byte("SELECT 1"))

		// check ensureVersionTable
		if err := d.(*MSSQL).ensureVersionTable(); err != nil {
			t.Fatal(err)
		}
		// check again
		if err := d.(*MSSQL).ensureVersionTable(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestLockWorks(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.Port(defaultPort)
		if err != nil {
			t.Fatal(err)
		}

		addr := fmt.Sprintf("sqlserver://sa:%v@%v:%v?master", saPassword, ip, port)
		p := &MSSQL{}
		d, err := p.Open(addr)
		if err != nil {
			t.Fatalf("%v", err)
		}
		dt.Test(t, d, []byte("SELECT 1"))

		ms := d.(*MSSQL)

		err = ms.Lock()
		if err != nil {
			t.Fatal(err)
		}
		err = ms.Unlock()
		if err != nil {
			t.Fatal(err)
		}

		// make sure the 2nd lock works (RELEASE_LOCK is very finicky)
		err = ms.Lock()
		if err != nil {
			t.Fatal(err)
		}
		err = ms.Unlock()
		if err != nil {
			t.Fatal(err)
		}
	})
}
