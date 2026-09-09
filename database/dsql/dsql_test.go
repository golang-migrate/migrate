package dsql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"strings"
	"testing"

	"github.com/dhui/dktest"
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
	dt "github.com/golang-migrate/migrate/v4/database/testing"
	"github.com/golang-migrate/migrate/v4/dktesting"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

const postgresPassword = "postgres"

var postgresSpec = dktesting.ContainerSpec{
	ImageName: "postgres:16",
	Options: dktest.Options{
		Env:          map[string]string{"POSTGRES_PASSWORD": postgresPassword},
		PortRequired: true,
		ReadyFunc:    postgresReady,
	},
}

func postgresReady(ctx context.Context, container dktest.ContainerInfo) bool {
	host, port, err := container.FirstPort()
	if err != nil {
		return false
	}
	db, err := sql.Open("pgx/v5", strings.Replace(postgresURL(host, port), "dsql://", "postgres://", 1))
	if err != nil {
		return false
	}
	defer func() {
		if err := db.Close(); err != nil {
			log.Println(err)
		}
	}()
	return db.PingContext(ctx) == nil
}

func postgresURL(host, port string, options ...string) string {
	options = append(options, "sslmode=disable")
	return fmt.Sprintf("dsql://postgres:%s@%s:%s/postgres?%s", postgresPassword, host, port, strings.Join(options, "&"))
}

func Test(t *testing.T) {
	dktesting.ParallelTest(t, []dktesting.ContainerSpec{postgresSpec}, func(t *testing.T, container dktest.ContainerInfo) {
		host, port, err := container.FirstPort()
		if err != nil {
			t.Fatal(err)
		}

		driver, err := (&DSQL{}).Open(postgresURL(host, port))
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := driver.Close(); err != nil {
				t.Error(err)
			}
		}()

		dt.Test(t, driver, []byte("SELECT 1"))
	})
}

func TestMigrate(t *testing.T) {
	dktesting.ParallelTest(t, []dktesting.ContainerSpec{postgresSpec}, func(t *testing.T, container dktest.ContainerInfo) {
		host, port, err := container.FirstPort()
		if err != nil {
			t.Fatal(err)
		}

		driver, err := (&DSQL{}).Open(postgresURL(host, port))
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := driver.Close(); err != nil {
				t.Error(err)
			}
		}()

		migration, err := migrate.NewWithDatabaseInstance("file://./examples/migrations", "dsql", driver)
		if err != nil {
			t.Fatal(err)
		}
		dt.TestMigrate(t, migration)
	})
}

func TestRunSplitsDDLStatements(t *testing.T) {
	dktesting.ParallelTest(t, []dktesting.ContainerSpec{postgresSpec}, func(t *testing.T, container dktest.ContainerInfo) {
		host, port, err := container.FirstPort()
		if err != nil {
			t.Fatal(err)
		}

		driver, err := (&DSQL{}).Open(postgresURL(host, port, "x-multi-statement=true"))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = driver.Close() }()

		if err := driver.Run(strings.NewReader("CREATE TABLE first_table (id BIGINT PRIMARY KEY); CREATE TABLE second_table (id BIGINT PRIMARY KEY);")); err != nil {
			t.Fatal(err)
		}

		var count int
		err = driver.(*DSQL).conn.QueryRowContext(context.Background(), `
			SELECT COUNT(1) FROM information_schema.tables
			WHERE table_schema = 'public' AND table_name IN ('first_table', 'second_table')
		`).Scan(&count)
		if err != nil {
			t.Fatal(err)
		}
		if count != 2 {
			t.Fatalf("expected 2 tables, got %d", count)
		}
	})
}

func TestLockIsSharedBetweenInstances(t *testing.T) {
	dktesting.ParallelTest(t, []dktesting.ContainerSpec{postgresSpec}, func(t *testing.T, container dktest.ContainerInfo) {
		host, port, err := container.FirstPort()
		if err != nil {
			t.Fatal(err)
		}
		url := postgresURL(host, port)

		first, err := (&DSQL{}).Open(url)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = first.Close() }()
		second, err := (&DSQL{}).Open(url)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = second.Close() }()

		if err := first.Lock(); err != nil {
			t.Fatal(err)
		}
		if err := second.Lock(); !errors.Is(err, database.ErrLocked) {
			t.Fatalf("expected ErrLocked, got %v", err)
		}
		if err := first.Unlock(); err != nil {
			t.Fatal(err)
		}
		if err := second.Lock(); err != nil {
			t.Fatal(err)
		}
		if err := second.Unlock(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestFilterCustomQuery(t *testing.T) {
	dktesting.ParallelTest(t, []dktesting.ContainerSpec{postgresSpec}, func(t *testing.T, container dktest.ContainerInfo) {
		host, port, err := container.FirstPort()
		if err != nil {
			t.Fatal(err)
		}
		driver, err := (&DSQL{}).Open(postgresURL(host, port, "x-migrations-table=custom_migrations", "x-lock-table=custom_lock"))
		if err != nil {
			t.Fatal(err)
		}
		_ = driver.Close()
	})
}

func TestDisableMultiStatementParsing(t *testing.T) {
	dktesting.ParallelTest(t, []dktesting.ContainerSpec{postgresSpec}, func(t *testing.T, container dktest.ContainerInfo) {
		host, port, err := container.FirstPort()
		if err != nil {
			t.Fatal(err)
		}
		driver, err := (&DSQL{}).Open(postgresURL(host, port, "x-multi-statement=false"))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = driver.Close() }()

		if err := driver.Run(strings.NewReader("SELECT 'value;with;semicolons';")); err != nil {
			t.Fatal(err)
		}
	})
}
