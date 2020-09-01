package vertica

import (
	"bytes"
	"context"
	"database/sql"
	sqldriver "database/sql/driver"
	"fmt"
	"log"
	"testing"

	"github.com/dhui/dktest"
	_ "github.com/vertica/vertica-sql-go"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
	dt "github.com/golang-migrate/migrate/v4/database/testing"
	"github.com/golang-migrate/migrate/v4/dktesting"

	_ "github.com/golang-migrate/migrate/v4/source/file"
)

const defaultPort = 5433

var (
	opts = dktest.Options{
		Env:          map[string]string{"DATABASE_PASSWORD": "password"},
		PortRequired: true,
		ReadyFunc:    isReady,
	}
	specs = []dktesting.ContainerSpec{
		{ImageName: "jbfavre/vertica:9.2.0-7_debian-8", Options: opts},
	}
)

func isReady(ctx context.Context, c dktest.ContainerInfo) bool {
	ip, port, err := c.Port(defaultPort)
	if err != nil {
		return false
	}

	db, err := sql.Open("vertica", fmt.Sprintf(`vertica://dbadmin:password@%s:%s/docker`, ip, port))
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

func TestParseDbName(t *testing.T) {

	cases := []struct {
		input string
		exp   string
	}{
		{
			"vertica://dbadmin:password@localhost:5433/mydb?x-migrations-table=lolo",
			"mydb",
		},
		{
			"vertica://dbadmin:password@localhost:5433/docker",
			"docker",
		},
		{
			"",
			"",
		},
	}
	for _, c := range cases {
		n := parseDbName(c.input)
		if n != c.exp {
			t.Logf(`parseDbName should return %v and returned %v`, c.exp, n)
		}
	}

}

func Test(t *testing.T) {

	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		addr := getVerticaURL(t, c)
		p := &Vertica{}
		d, err := p.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()
		dt.TestNilVersion(t, d) // test first
		// dt.TestLockAndUnlock(t, d)
		dt.TestRun(t, d, bytes.NewReader([]byte("SELECT 1")))
		dt.TestSetVersion(t, d) // also tests Version()
		// Drop breaks the driver, so test it last.
		dt.TestDrop(t, d)
	})
}

func getVerticaURL(t *testing.T, c dktest.ContainerInfo) string {
	ip, port, err := c.Port(defaultPort)
	if err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf("vertica://dbadmin:password@%v:%v/docker", ip, port)
}

func TestOpen(t *testing.T) {
	// Make sure the driver is registered.
	// But if the previous test already registered it just ignore the panic.
	// If we don't do this it will be impossible to run this test standalone.
	func() {
		defer func() {
			_ = recover()
		}()
		database.Register("vertica", &Vertica{})
	}()

	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {

		addr := getVerticaURL(t, c)
		cases := []struct {
			url string
			err bool
		}{
			{
				addr + "?x-migrations-table=lolo",
				false,
			},
			{
				"unknown://bla",
				true,
			},
		}

		for _, c := range cases {
			t.Run(c.url, func(t *testing.T) {
				v := &Vertica{}
				d, err := v.Open(c.url)

				if err == nil {
					if c.err {
						t.Fatal("expected an error for an unknown driver")
					} else {
						if vd, ok := d.(*Vertica); !ok {
							t.Fatalf("expected *Vertica got %T", d)
						} else if vd.config.MigrationsTable != "lolo" || vd.config.DatabaseName != "docker" {
							t.Fatalf("expected %q got %q or expected db name be docker got %q", "lolo", vd.config.MigrationsTable, vd.config.DatabaseName)
						}
					}
				} else if !c.err {
					t.Fatalf("did not expect %q", err)
				}
			})
		}
	})
}

func TestMigrate(t *testing.T) {

	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {

		addr := getVerticaURL(t, c)
		p := &Vertica{}
		d, err := p.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()
		m, err := migrate.NewWithDatabaseInstance("file://./examples/migrations", "docker", d)
		if err != nil {
			t.Fatal(err)
		}
		dt.TestMigrate(t, m)

	})
}
