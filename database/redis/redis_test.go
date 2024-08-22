package redis

import (
	"context"
	"fmt"
	"github.com/dhui/dktest"
	"github.com/golang-migrate/migrate/v4"
	dt "github.com/golang-migrate/migrate/v4/database/testing"
	"github.com/golang-migrate/migrate/v4/dktesting"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/redis/go-redis/v9"
	"log"
	"net"
	"strings"
	"testing"
)

const (
	redisPassword = "password"
)

var (
	opts = dktest.Options{
		Env:          map[string]string{"REDIS_PASSWORD": redisPassword},
		PortRequired: true,
		ReadyFunc:    isReady,
	}
	specs = []dktesting.ContainerSpec{
		{ImageName: "bitnami/redis:6.2", Options: opts},
		{ImageName: "bitnami/redis:7.4", Options: opts},
	}
)

func redisConnectionString(host, port string, options ...string) string {
	return fmt.Sprintf("redis://:%s@%s/0?%s", redisPassword, net.JoinHostPort(host, port), strings.Join(options, "&"))
}

func isReady(ctx context.Context, c dktest.ContainerInfo) bool {
	ip, port, err := c.FirstPort()
	if err != nil {
		return false
	}

	client := redis.NewClient(&redis.Options{
		Network:               "tcp",
		Addr:                  net.JoinHostPort(ip, port),
		Password:              redisPassword,
		ContextTimeoutEnabled: true,
		DisableIndentity:      true,
	})

	defer func() {
		if err := client.Close(); err != nil {
			log.Println("close error:", err)
		}
	}()

	err = client.Ping(ctx).Err()

	return err == nil
}

func Test(t *testing.T) {
	t.Run("test", test)
	t.Run("testMigrate", testMigrate)
	t.Run("testErrorParsing", testErrorParsing)
	t.Run("testFilterCustomQuery", testFilterCustomQuery)
	t.Run("testMigrationsKeyOption", testMigrationsKeyOption)
	t.Run("testRedisLock", testRedisLock)

	t.Cleanup(func() {
		for _, spec := range specs {
			t.Log("Cleaning up ", spec.ImageName)
			if err := spec.Cleanup(); err != nil {
				t.Error("Error removing ", spec.ImageName, "error:", err)
			}
		}
	})
}

func test(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.FirstPort()
		if err != nil {
			t.Fatal(err)
		}

		addr := redisConnectionString(ip, port)
		r := &Redis{}
		d, err := r.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()
		dt.Test(t, d, []byte("return 1"))
	})
}

func testMigrate(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.FirstPort()
		if err != nil {
			t.Fatal(err)
		}

		addr := redisConnectionString(ip, port)
		r := &Redis{}
		d, err := r.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()
		m, err := migrate.NewWithDatabaseInstance("file://./examples/migrations", "redis", d)
		if err != nil {
			t.Fatal(err)
		}
		dt.TestMigrate(t, m)
	})
}

func testErrorParsing(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.FirstPort()
		if err != nil {
			t.Fatal(err)
		}

		addr := redisConnectionString(ip, port)
		p := &Redis{}
		d, err := p.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()

		wantErrSubstring := "Script attempted to access nonexistent global variable 'asdad'"
		if err := d.Run(strings.NewReader("return asdad")); err == nil {
			t.Fatal("expected err but got nil")
		} else if !strings.Contains(err.Error(), "Script attempted to access nonexistent global variable 'asdad'") {
			t.Fatalf("expected substring '%s' but got '%s'", wantErrSubstring, err.Error())
		}
	})
}

func testFilterCustomQuery(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.FirstPort()
		if err != nil {
			t.Fatal(err)
		}

		addr := redisConnectionString(ip, port, "x-custom=foobar")
		r := &Redis{}
		d, err := r.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()
	})
}

func testMigrationsKeyOption(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.FirstPort()
		if err != nil {
			t.Fatal(err)
		}

		r := &Redis{}

		migrationsKey := "my_migrations"

		// good quoted x-migrations-table parameter
		d, err := r.Open(redisConnectionString(ip, port, fmt.Sprintf("x-migrations-key=%s", migrationsKey)))
		if err != nil {
			t.Fatal(err)
		}

		defer func() {
			if err := d.Close(); err != nil {
				t.Fatal(err)
			}
		}()

		m, err := migrate.NewWithDatabaseInstance("file://./examples/migrations", "redis", d)
		if err != nil {
			t.Fatal(err)
		}

		if err = m.Up(); err != nil {
			t.Fatal(err)
		}

		// NOTE: redis create migrations hash during first migration automatically.
		existsCount, err := d.(*Redis).client.Exists(context.Background(), migrationsKey).Result()
		if err != nil {
			t.Fatal(err)
		}
		if existsCount == 0 {
			t.Fatalf("expected key %s not exist", migrationsKey)
		}
	})
}

func testRedisLock(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.FirstPort()
		if err != nil {
			t.Fatal(err)
		}

		addr := redisConnectionString(ip, port)
		p := &Redis{}
		d, err := p.Open(addr)
		if err != nil {
			t.Fatal(err)
		}

		dt.Test(t, d, []byte("return 1"))

		ps := d.(*Redis)

		err = ps.Lock()
		if err != nil {
			t.Fatal(err)
		}

		err = ps.Unlock()
		if err != nil {
			t.Fatal(err)
		}

		err = ps.Lock()
		if err != nil {
			t.Fatal(err)
		}

		err = ps.Unlock()
		if err != nil {
			t.Fatal(err)
		}
	})
}
