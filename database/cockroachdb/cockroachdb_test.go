package cockroachdb

// error codes https://github.com/lib/pq/blob/master/error.go

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
	"github.com/pkg/errors"
	"log"
	"regexp"
	"strings"
	"testing"
)

import (
	"github.com/dhui/dktest"
	_ "github.com/lib/pq"
)

import (
	dt "github.com/golang-migrate/migrate/v4/database/testing"
	"github.com/golang-migrate/migrate/v4/dktesting"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

const defaultPort = 26257

var (
	opts = dktest.Options{Cmd: []string{"start-single-node", "--insecure"}, PortRequired: true, ReadyFunc: isReady}
	// Released versions: https://www.cockroachlabs.com/docs/releases/
	specs = []dktesting.ContainerSpec{
		{ImageName: "cockroachdb/cockroach:latest-v22.1", Options: opts},
		{ImageName: "cockroachdb/cockroach:latest-v22.2", Options: opts},
		{ImageName: "cockroachdb/cockroach:latest-v23.1", Options: opts},
	}
)

func isReady(ctx context.Context, c dktest.ContainerInfo) bool {
	ip, port, err := c.Port(defaultPort)
	if err != nil {
		log.Println("port error:", err)
		return false
	}

	db, err := sql.Open("postgres", fmt.Sprintf("postgres://root@%v:%v?sslmode=disable", ip, port))
	if err != nil {
		log.Println("open error:", err)
		return false
	}
	if err := db.PingContext(ctx); err != nil {
		log.Println("ping error:", err)
		return false
	}
	if err := db.Close(); err != nil {
		log.Println("close error:", err)
	}
	return true
}

func createDB(t *testing.T, c dktest.ContainerInfo) {
	ip, port, err := c.Port(defaultPort)
	if err != nil {
		t.Fatal(err)
	}

	db, err := sql.Open("postgres", fmt.Sprintf("postgres://root@%v:%v?sslmode=disable", ip, port))
	if err != nil {
		t.Fatal(err)
	}
	if err = db.Ping(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	}()

	if _, err = db.Exec("CREATE DATABASE migrate"); err != nil {
		t.Fatal(err)
	}
}

func mustRun(t *testing.T, d database.Driver, statements []string) {
	for _, statement := range statements {
		if err := d.Run(strings.NewReader(statement)); err != nil {
			t.Fatal(err)
		}
	}
}

func Test(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, ci dktest.ContainerInfo) {
		createDB(t, ci)

		ip, port, err := ci.Port(26257)
		if err != nil {
			t.Fatal(err)
		}

		addr := fmt.Sprintf("cockroach://root@%v:%v/migrate?sslmode=disable", ip, port)
		c := &CockroachDb{}
		d, err := c.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		dt.Test(t, d, []byte("SELECT 1"))
	})
}

func TestMigrate(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, ci dktest.ContainerInfo) {
		createDB(t, ci)

		ip, port, err := ci.Port(26257)
		if err != nil {
			t.Fatal(err)
		}

		addr := fmt.Sprintf("cockroach://root@%v:%v/migrate?sslmode=disable", ip, port)
		c := &CockroachDb{}
		d, err := c.Open(addr)
		if err != nil {
			t.Fatal(err)
		}

		m, err := migrate.NewWithDatabaseInstance("file://./examples/migrations", "migrate", d)
		if err != nil {
			t.Fatal(err)
		}
		dt.TestMigrate(t, m)
	})
}

func TestMultiStatement(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, ci dktest.ContainerInfo) {
		createDB(t, ci)

		ip, port, err := ci.Port(26257)
		if err != nil {
			t.Fatal(err)
		}

		addr := fmt.Sprintf("cockroach://root@%v:%v/migrate?sslmode=disable", ip, port)
		c := &CockroachDb{}
		d, err := c.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		if err := d.Run(strings.NewReader("CREATE TABLE foo (foo text); CREATE TABLE bar (bar text);")); err != nil {
			t.Fatalf("expected err to be nil, got %v", err)
		}

		// make sure second table exists
		var exists bool
		if err := d.(*CockroachDb).db.QueryRow("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'bar' AND table_schema = (SELECT current_schema()))").Scan(&exists); err != nil {
			t.Fatal(err)
		}
		if !exists {
			t.Fatalf("expected table bar to exist")
		}
	})
}

func TestFilterCustomQuery(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, ci dktest.ContainerInfo) {
		createDB(t, ci)

		ip, port, err := ci.Port(26257)
		if err != nil {
			t.Fatal(err)
		}

		addr := fmt.Sprintf("cockroach://root@%v:%v/migrate?sslmode=disable&x-custom=foobar", ip, port)
		c := &CockroachDb{}
		_, err = c.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
	})
}

func TestRole(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, ci dktest.ContainerInfo) {
		createDB(t, ci)

		ip, port, err := ci.Port(26257)
		if err != nil {
			t.Fatal(err)
		}

		d, err := sql.Open("postgres", fmt.Sprintf("postgres://root@%v:%v?sslmode=disable", ip, port))
		if err != nil {
			t.Fatal(err)
		}
		prepare := []string{
			"CREATE ROLE IF NOT EXISTS _fa NOLOGIN;",
			"CREATE ROLE IF NOT EXISTS _fa_ungranted NOLOGIN",
			"CREATE ROLE deploy LOGIN",
			"GRANT _fa TO deploy",
			"GRANT CREATE ON DATABASE migrate TO _fa, _fa_ungranted;",
		}
		for _, query := range prepare {
			if _, err := d.Exec(query); err != nil {
				t.Fatal(err)
			}
		}

		c := &CockroachDb{}

		// positive: connecting with deploy user and setting role to _fa
		d2, err := c.Open(fmt.Sprintf("cockroach://deploy@%v:%v/migrate?sslmode=disable&x-role=_fa", ip, port))
		if err != nil {
			t.Fatal(err)
		}
		mustRun(t, d2, []string{
			"CREATE TABLE foo (role INT UNIQUE);",
		})
		var exists bool
		if err := d2.(*CockroachDb).db.QueryRow("SELECT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'foo' AND schemaname = (SELECT current_schema()) AND tableowner = '_fa');").Scan(&exists); err != nil {
			t.Fatal(err)
		}
		if !exists {
			t.Fatalf("expected table foo owned by _fa role to exist")
		}

		var e *database.Error
		// negative: connecting with deploy user and trying to set not existing role
		_, err = c.Open(fmt.Sprintf("cockroach://root@%v:%v/migrate?sslmode=disable&x-role=_not_existing_role", ip, port))
		if !errors.As(err, &e) || err == nil {
			t.Fatal(fmt.Errorf("unexpected success, wanted pq: role/user does not exist, got: %w", err))
		}
		re := regexp.MustCompile("^pq: role(/user)? (\")?_not_existing_role(\")? does not exist$")
		if !re.MatchString(e.OrigErr.Error()) {
			t.Fatal(fmt.Errorf("unexpected error, wanted _not_existing_role does not exist, got: %s", e.OrigErr.Error()))
		}

		// negative: connecting with deploy user and trying to set not granted role
		_, err = c.Open(fmt.Sprintf("cockroach://deploy@%v:%v/migrate?sslmode=disable&x-role=_fa_ungranted", ip, port))
		if !errors.As(err, &e) || err == nil {
			t.Fatal(fmt.Errorf("unexpected success, wanted permission denied error, got: %w", err))
		}
		if !strings.Contains(e.OrigErr.Error(), "permission denied to set role") {
			t.Fatal(fmt.Errorf("unexpected error, wanted permission denied error, got: %s", e.OrigErr.Error()))
		}
	})
}
