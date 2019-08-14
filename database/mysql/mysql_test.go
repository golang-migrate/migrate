package mysql

import (
	"context"
	"database/sql"
	sqldriver "database/sql/driver"
	"fmt"
	"log"

	"github.com/golang-migrate/migrate/v4"
	"testing"
)

import (
	"github.com/dhui/dktest"
	"github.com/go-sql-driver/mysql"
)

import (
	dt "github.com/golang-migrate/migrate/v4/database/testing"
	"github.com/golang-migrate/migrate/v4/dktesting"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

const defaultPort = 3306

var (
	opts = dktest.Options{
		Env:          map[string]string{"MYSQL_ROOT_PASSWORD": "root", "MYSQL_DATABASE": "public"},
		PortRequired: true, ReadyFunc: isReady,
	}
	// Supported versions: https://www.mysql.com/support/supportedplatforms/database.html
	specs = []dktesting.ContainerSpec{
		{ImageName: "mysql:5.5", Options: opts},
		{ImageName: "mysql:5.6", Options: opts},
		{ImageName: "mysql:5.7", Options: opts},
		{ImageName: "mysql:8", Options: opts},
	}
)

func isReady(ctx context.Context, c dktest.ContainerInfo) bool {
	ip, port, err := c.Port(defaultPort)
	if err != nil {
		return false
	}

	db, err := sql.Open("mysql", fmt.Sprintf("root:root@tcp(%v:%v)/public", ip, port))
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
		case sqldriver.ErrBadConn, mysql.ErrInvalidConn:
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

		addr := fmt.Sprintf("mysql://root:root@tcp(%v:%v)/public", ip, port)
		p := &Mysql{}
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

		// check ensureVersionTable
		if err := d.(*Mysql).ensureVersionTable(); err != nil {
			t.Fatal(err)
		}
		// check again
		if err := d.(*Mysql).ensureVersionTable(); err != nil {
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

		addr := fmt.Sprintf("mysql://root:root@tcp(%v:%v)/public", ip, port)
		p := &Mysql{}
		d, err := p.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()

		m, err := migrate.NewWithDatabaseInstance("file://./examples/migrations", "public", d)
		if err != nil {
			t.Fatal(err)
		}
		dt.TestMigrate(t, m, []byte("SELECT 1"))

		// check ensureVersionTable
		if err := d.(*Mysql).ensureVersionTable(); err != nil {
			t.Fatal(err)
		}
		// check again
		if err := d.(*Mysql).ensureVersionTable(); err != nil {
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

		addr := fmt.Sprintf("mysql://root:root@tcp(%v:%v)/public", ip, port)
		p := &Mysql{}
		d, err := p.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		dt.Test(t, d, []byte("SELECT 1"))

		ms := d.(*Mysql)

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
