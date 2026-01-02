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

const user = "user"
const pass = "pass"

var (
	opts = dktest.Options{
		Entrypoint: []string{""},
		Cmd:        []string{"/surreal", "start", "--user", user, "--pass", pass, "memory"},
		ExposedPorts: map[nat.Port]struct{}{
			"8000/tcp": {},
		},
		PortRequired: true,
		ReadyFunc:    isReady,
	}
	specs = []dktesting.ContainerSpec{
		{ImageName: "surrealdb/surrealdb:v2.4", Options: opts},
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

		badQuery := "DEFINE TABLEE user SCHEMALESS;"
		wantErr := `There was a problem with the database: Parse error:`
		if err := d.Run(strings.NewReader(badQuery)); err == nil {
			t.Fatal("expected err but got nil")
		} else if !strings.Contains(err.Error(), wantErr) {
			t.Fatalf("expected '%s' to contain '%s'", err.Error(), wantErr)
		} else if !strings.Contains(err.Error(), badQuery) {
			t.Fatalf("expected err to contain %s", badQuery)
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

		db, err := surrealdb.FromEndpointURLString(t.Context(), connInfo.getUrl())
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			err = db.Close(t.Context())
			if err != nil {
				t.Fatal(err)
			}
		}()

		_, err = db.SignIn(t.Context(), &surrealdb.Auth{
			Username: "user",
			Password: "pass",
		})
		if err != nil {
			t.Fatal(err)
		}

		err = db.Use(t.Context(), connInfo.NS, connInfo.DB)
		if err != nil {
			t.Fatal(err)
		}

		_, err = surrealdb.Query[[]any](t.Context(), db, "SELECT * FROM schema_migrations:version", map[string]any{})
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
	db, err := surrealdb.FromEndpointURLString(ctx, connInfo.getUrl())
	if err != nil {
		return false
	}
	defer func() {
		err = db.Close(ctx)
		if err != nil {
			panic(err)
		}
	}()

	_, err = db.SignIn(ctx, &surrealdb.Auth{
		Username: "user",
		Password: "pass",
	})
	if err != nil {
		return false
	}

	err = db.Use(ctx, connInfo.NS, connInfo.DB)
	if err != nil {
		return false
	}

	_, err = surrealdb.Query[[]any](ctx, db, `SELECT * FROM 1;`, map[string]any{})
	return err == nil
}
