package clickhouse_test

import (
	"context"
	"database/sql"
	sqldriver "database/sql/driver"
	"fmt"
	"log"
	"testing"

	_ "github.com/ClickHouse/clickhouse-go"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/clickhouse"
	dt "github.com/golang-migrate/migrate/v4/database/testing"

	"github.com/testcontainers/testcontainers-go"
	tcclickhouse "github.com/testcontainers/testcontainers-go/modules/clickhouse"

	_ "github.com/golang-migrate/migrate/v4/source/file"
)

var (
	tableEngines = []string{"TinyLog", "MergeTree"}
	specs        = []string{
		"yandex/clickhouse-server:21.3",
	}
)

func startClickHouse(spec string, engine string) (*tcclickhouse.ClickHouseContainer, error) {
	c, err := tcclickhouse.Run(context.Background(), spec, testcontainers.WithEnv(map[string]string{
		"CLICKHOUSE_USER":     "user",
		"CLICKHOUSE_PASSWORD": "password",
		"CLICKHOUSE_DB":       "db",
	}))
	if err != nil {
		return nil, err
	}

	// from here, return the container and the error, so the client code
	// can defer the container.Terminate call

	connectionString, err := c.ConnectionString(context.Background(), extraArgs(engine))
	if err != nil {
		return c, fmt.Errorf("failed to get connection string: %s", err)
	}

	db, err := sql.Open("clickhouse", connectionString)

	if err != nil {
		return c, fmt.Errorf("failed to open connection: %s", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			log.Println("close error:", err)
		}
	}()

	if err = db.PingContext(context.Background()); err != nil {
		switch err {
		case sqldriver.ErrBadConn:
			return c, fmt.Errorf("failed to ping: %s", err)
		default:
			fmt.Println(err)
		}
		return c, fmt.Errorf("failed to ping: %s", err)
	}

	return c, nil
}

func extraArgs(engine string) string {
	extraArgs := "username=user&password=password&database=db&x-multi-statement=true&debug=false"
	if engine != "" {
		extraArgs += "&x-migrations-table-engine=" + engine
	}

	return extraArgs
}

func TestCases(t *testing.T) {
	for _, engine := range tableEngines {
		t.Run("Test_"+engine, func(t *testing.T) { testSimple(t, engine) })
		t.Run("Migrate_"+engine, func(t *testing.T) { testMigrate(t, engine) })
		t.Run("Version_"+engine, func(t *testing.T) { testVersion(t, engine) })
		t.Run("Drop_"+engine, func(t *testing.T) { testDrop(t, engine) })
	}
	t.Run("WithInstanceDefaultConfigValues", func(t *testing.T) { testSimpleWithInstanceDefaultConfigValues(t) })
}

func testSimple(t *testing.T, engine string) {
	for _, spec := range specs {
		spec := spec // capture range variable, see https://goo.gl/60w3p2
		t.Run(spec, func(t *testing.T) {
			c, err := startClickHouse(spec, engine)
			if err != nil {
				t.Fatal(err)
			}

			connectionString, err := c.ConnectionString(context.Background(), extraArgs(engine))
			if err != nil {
				t.Fatal(err)
			}

			p := &clickhouse.ClickHouse{}
			d, err := p.Open(connectionString)
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
}

func testSimpleWithInstanceDefaultConfigValues(t *testing.T) {
	const engine string = "" // empty engine for default config values

	for _, spec := range specs {
		spec := spec // capture range variable, see https://goo.gl/60w3p2
		t.Run(spec, func(t *testing.T) {
			c, err := startClickHouse(spec, engine)
			if err != nil {
				t.Fatal(err)
			}

			connectionString, err := c.ConnectionString(context.Background(), extraArgs(engine))
			if err != nil {
				t.Fatal(err)
			}

			conn, err := sql.Open("clickhouse", connectionString)
			if err != nil {
				t.Fatal(err)
			}
			d, err := clickhouse.WithInstance(conn, &clickhouse.Config{})
			if err != nil {
				_ = conn.Close()
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
}

func testMigrate(t *testing.T, engine string) {
	for _, spec := range specs {
		spec := spec // capture range variable, see https://goo.gl/60w3p2
		t.Run(spec, func(t *testing.T) {
			c, err := startClickHouse(spec, engine)
			if err != nil {
				t.Fatal(err)
			}

			connectionString, err := c.ConnectionString(context.Background(), extraArgs(engine))
			if err != nil {
				t.Fatal(err)
			}

			p := &clickhouse.ClickHouse{}
			d, err := p.Open(connectionString)
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
}

func testVersion(t *testing.T, engine string) {
	for _, spec := range specs {
		spec := spec // capture range variable, see https://goo.gl/60w3p2
		t.Run(spec, func(t *testing.T) {
			expectedVersion := 1

			c, err := startClickHouse(spec, engine)
			if err != nil {
				t.Fatal(err)
			}

			connectionString, err := c.ConnectionString(context.Background(), extraArgs(engine))
			if err != nil {
				t.Fatal(err)
			}

			p := &clickhouse.ClickHouse{}
			d, err := p.Open(connectionString)
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
}

func testDrop(t *testing.T, engine string) {
	for _, spec := range specs {
		spec := spec // capture range variable, see https://goo.gl/60w3p2
		t.Run(spec, func(t *testing.T) {
			c, err := startClickHouse(spec, engine)
			if err != nil {
				t.Fatal(err)
			}

			connectionString, err := c.ConnectionString(context.Background(), extraArgs(engine))
			if err != nil {
				t.Fatal(err)
			}

			p := &clickhouse.ClickHouse{}
			d, err := p.Open(connectionString)
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
}
