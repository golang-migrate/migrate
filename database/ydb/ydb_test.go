package ydb

import (
	"context"
	"fmt"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/dhui/dktest"
	"github.com/docker/go-connections/nat"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/balancers"

	"github.com/golang-migrate/migrate/v4"
	dt "github.com/golang-migrate/migrate/v4/database/testing"
	"github.com/golang-migrate/migrate/v4/dktesting"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

const (
	defaultPort  = 2136
	databaseName = "local"
)

var (
	getOptions = func(port string) dktest.Options {
		return dktest.Options{
			Env: map[string]string{
				"GRPC_TLS_PORT": "2135",
				"GRPC_PORT":     "2136",
				"MON_PORT":      "8765",
			},
			PortBindings: nat.PortMap{
				nat.Port(fmt.Sprintf("%d/tcp", defaultPort)): []nat.PortBinding{
					{
						HostIP:   "0.0.0.0",
						HostPort: port,
					},
				},
			},
			PortRequired: true,
			Hostname:     "127.0.0.1",
			ReadyTimeout: 15 * time.Second,
			ReadyFunc:    isReady,
		}
	}

	// Released version: https://ydb.tech/docs/downloads/#ydb-server
	specs = []dktesting.ContainerSpec{
		{ImageName: "ydbplatform/local-ydb:latest", Options: getOptions("22000")},
		{ImageName: "ydbplatform/local-ydb:24.3", Options: getOptions("22001")},
		{ImageName: "ydbplatform/local-ydb:24.2", Options: getOptions("22002")},
	}
)

func connectionString(host, port string, options ...string) string {
	return fmt.Sprintf("ydb://%s:%s/%s?%s", host, port, databaseName, strings.Join(options, "&"))
}

func isReady(ctx context.Context, c dktest.ContainerInfo) bool {
	ip, port, err := c.Port(defaultPort)
	if err != nil {
		log.Println("port error:", err)
		return false
	}

	d, err := ydb.Open(ctx, fmt.Sprintf("grpc://%s:%s/%s", ip, port, databaseName), ydb.WithBalancer(balancers.SingleConn()))
	if err != nil {
		return false
	}
	defer func() { _ = d.Close(ctx) }()

	err = d.Query().Exec(ctx, `
		CREATE TABLE test (
		id Int,
		PRIMARY KEY(id)
	);
	DROP TABLE test;`, nil)
	return err == nil
}

func Test(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.Port(defaultPort)
		if err != nil {
			t.Fatal(err)
		}

		db := &YDB{}
		d, err := db.Open(connectionString(ip, port))
		if err != nil {
			t.Fatal(err)
		}

		defer func() {
			err := d.Close()
			if err != nil {
				t.Fatal(err)
			}
		}()

		dt.Test(t, d, []byte("CREATE TABLE `a/b/c/d/table` (x Uint64 NOT NULL, PRIMARY KEY (x))"))
	})
}

func TestMigrate(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.Port(defaultPort)
		if err != nil {
			t.Fatal(err)
		}

		db := &YDB{}
		d, err := db.Open(connectionString(ip, port))
		if err != nil {
			t.Fatal(err)
		}

		defer func() {
			err := d.Close()
			if err != nil {
				t.Fatal(err)
			}
		}()

		m, err := migrate.NewWithDatabaseInstance("file://./examples/migrations", "ydb", d)
		if err != nil {
			t.Fatal(err)
		}

		dt.TestMigrate(t, m)
	})
}
