package surrealdb

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/docker/go-connections/nat"
	"github.com/golang-migrate/migrate/v4"

	"github.com/dhui/dktest"

	dt "github.com/golang-migrate/migrate/v4/database/testing"
	"github.com/golang-migrate/migrate/v4/dktesting"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/surrealdb/surrealdb.go"
)

type ConnInfo struct {
	User string
	Pass string
	Host string
	Port string
	NS   string
	DB   string
}

func (c *ConnInfo) getUrl() string {
	return fmt.Sprintf("ws://%s:%s/rpc", c.Host, c.Port)
}

func (c *ConnInfo) connString(options ...string) string {
	options = append(options, "sslmode=disable")
	return fmt.Sprintf("surrealdb://%s:%s@%s:%s/%s/%s?%s", c.User, c.Pass, c.Host, c.Port, c.NS, c.DB, strings.Join(options, "&"))
}

func getPortBindings() map[nat.Port][]nat.PortBinding {
	_, portBindings, err := nat.ParsePortSpecs([]string{"8000/tcp"})
	if err != nil {
		panic("Error setting up port bindings")
	}
	return portBindings
}

const user = "user"
const pass = "pass"

var (
	opts = dktest.Options{
		Entrypoint:   []string{""},
		Cmd:          []string{"/surreal", "start", "--user", user, "--pass", pass, "memory"},
		PortBindings: getPortBindings(),
		PortRequired: true, ReadyFunc: isReady}
	specs = []dktesting.ContainerSpec{
		{ImageName: "surrealdb/surrealdb:v1.1.1", Options: opts},
	}
)

func Test(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.FirstPort()
		if err != nil {
			t.Fatal(err)
		}

		connInfo := getConnInfo(ip, port)
		sur := &SurrealDB{}
		d, err := sur.Open(connInfo.connString())
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()
		if err != nil {
			t.Fatal(err)
		}
		dt.Test(t, d, []byte("SELECT * FROM 1;"))
	})
}

func TestMigrate(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.FirstPort()
		if err != nil {
			t.Fatal(err)
		}

		connInfo := getConnInfo(ip, port)
		sur := &SurrealDB{}
		d, err := sur.Open(connInfo.connString())
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()
		if err != nil {
			t.Fatal(err)
		}

		m, err := migrate.NewWithDatabaseInstance("file://./examples/migrations", "surrealdb", d)
		if err != nil {
			t.Fatal(err)
		}
		dt.TestMigrate(t, m)
	})
}

func TestErrorParsing(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.FirstPort()
		if err != nil {
			t.Fatal(err)
		}

		connInfo := getConnInfo(ip, port)
		sur := &SurrealDB{}
		d, err := sur.Open(connInfo.connString())
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()
		if err != nil {
			t.Fatal(err)
		}

		wantErr := `There was a problem with the database: Parse error on line 1 at character 0 when parsing 'DEFINE TABLEE user SCHEMALESS;'`
		if err := d.Run(strings.NewReader("DEFINE TABLEE user SCHEMALESS;")); err == nil {
			t.Fatal("expected err but got nil")
		} else if err.Error() != wantErr {
			t.Fatalf("expected '%s' but got '%s'", wantErr, err.Error())
		}
	})
}

func TestFilterCustomQuery(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.FirstPort()
		if err != nil {
			t.Fatal(err)
		}

		connInfo := getConnInfo(ip, port)
		sur := &SurrealDB{}
		d, err := sur.Open(connInfo.connString("x-custom=foobar"))
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()
		if err != nil {
			t.Fatal(err)
		}
	})
}

func TestMigrationTable(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.FirstPort()
		if err != nil {
			t.Fatal(err)
		}

		connInfo := getConnInfo(ip, port)
		sur := &SurrealDB{}
		d, err := sur.Open(connInfo.connString())
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()
		if err != nil {
			t.Fatal(err)
		}

		m, err := migrate.NewWithDatabaseInstance("file://./examples/migrations", "surrealdb", d)
		if err != nil {
			t.Fatal(err)
		}

		err = m.Up()
		if err != nil {
			t.Fatal(err)
		}

		db, err := surrealdb.New(connInfo.getUrl())
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		_, err = db.Signin(map[string]interface{}{
			"user": connInfo.User,
			"pass": connInfo.Pass,
		})
		if err != nil {
			t.Fatal(err)
		}

		_, err = db.Use(connInfo.NS, connInfo.DB)
		if err != nil {
			t.Fatal(err)
		}

		_, err = db.Query("SELECT * FROM schema_migrations:version", map[string]interface{}{})
		if err != nil {
			t.Fatal(err)
		}
	})
}

func TestParallelNamespace(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.FirstPort()
		if err != nil {
			t.Fatal(err)
		}

		connInfoFoo := getConnInfoForDB(ip, port, "foo", "foo")
		surFoo := &SurrealDB{}
		dfoo, err := surFoo.Open(connInfoFoo.connString())
		defer func() {
			err = dfoo.Close()
			if err != nil {
				t.Fatal(err)
			}
		}()
		if err != nil {
			t.Fatal(err)
		}

		connInfoBar := getConnInfoForDB(ip, port, "bar", "bar")
		surBar := &SurrealDB{}
		dbar, err := surBar.Open(connInfoBar.connString())
		defer func() {
			err = dbar.Close()
			if err != nil {
				t.Fatal(err)
			}
		}()
		if err != nil {
			t.Fatal(err)
		}

		if err := dfoo.Lock(); err != nil {
			t.Fatal(err)
		}

		if err := dbar.Lock(); err != nil {
			t.Fatal(err)
		}

		if err := dbar.Unlock(); err != nil {
			t.Fatal(err)
		}

		if err := dfoo.Unlock(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestParallelDatabase(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.FirstPort()
		if err != nil {
			t.Fatal(err)
		}

		connInfoFoo := getConnInfoForDB(ip, port, "foo", "foo")
		surFoo := &SurrealDB{}
		dfoo, err := surFoo.Open(connInfoFoo.connString())
		defer func() {
			err = dfoo.Close()
			if err != nil {
				t.Fatal(err)
			}
		}()
		if err != nil {
			t.Fatal(err)
		}

		connInfoBar := getConnInfoForDB(ip, port, "foo", "bar")
		surBar := &SurrealDB{}
		dbar, err := surBar.Open(connInfoBar.connString())
		defer func() {
			err = dbar.Close()
			if err != nil {
				t.Fatal(err)
			}
		}()
		if err != nil {
			t.Fatal(err)
		}

		if err := dfoo.Lock(); err != nil {
			t.Fatal(err)
		}

		if err := dbar.Lock(); err != nil {
			t.Fatal(err)
		}

		if err := dbar.Unlock(); err != nil {
			t.Fatal(err)
		}

		if err := dfoo.Unlock(); err != nil {
			t.Fatal(err)
		}
	})
}

///////////////////////////////////////
////////// Test Helper Funcs //////////
///////////////////////////////////////

func getConnInfoForDB(host string, port string, ns string, db string) ConnInfo {
	return ConnInfo{
		User: user,
		Pass: pass,
		Host: host,
		Port: port,
		NS:   ns,
		DB:   db,
	}
}

func getConnInfo(host string, port string) ConnInfo {
	return getConnInfoForDB(host, port, "test_ns", "test_db")
}

func isReady(ctx context.Context, c dktest.ContainerInfo) bool {
	ip, port, err := c.FirstPort()
	if err != nil {
		return false
	}

	connInfo := getConnInfo(ip, port)
	db, err := surrealdb.New(connInfo.getUrl())
	if err != nil {
		return false
	}
	defer db.Close()

	_, err = db.Signin(map[string]interface{}{
		"user": connInfo.User,
		"pass": connInfo.Pass,
	})
	if err != nil {
		return false
	}

	_, err = db.Use(connInfo.NS, connInfo.DB)
	if err != nil {
		return false
	}

	_, err = db.Query(`SELECT * FROM 1;`, map[string]interface{}{})
	return err == nil
}
