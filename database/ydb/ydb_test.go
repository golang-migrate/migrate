package ydb

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/dhui/dktest"
	"github.com/docker/go-connections/nat"
	"github.com/ydb-platform/ydb-go-sdk/v3"

	"github.com/golang-migrate/migrate/v4"
	dt "github.com/golang-migrate/migrate/v4/database/testing"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

const (
	image        = "ydbplatform/local-ydb:latest"
	host         = "localhost"
	port         = "2136"
	databaseName = "local"
)

var (
	opts = dktest.Options{
		Env: map[string]string{
			"GRPC_TLS_PORT": "2135",
			"GRPC_PORT":     "2136",
			"MON_PORT":      "8765",
		},

		PortBindings: nat.PortMap{
			nat.Port("2136/tcp"): []nat.PortBinding{
				{
					HostIP:   "0.0.0.0",
					HostPort: port,
				},
			},
		},

		Hostname:     host,
		ReadyTimeout: 15 * time.Second,
		ReadyFunc:    isReady,
	}
)

func connectionString(options ...string) string {
	return fmt.Sprintf("ydb://%s:%s/%s?%s", host, port, databaseName, strings.Join(options, "&"))
}

func isReady(ctx context.Context, c dktest.ContainerInfo) bool {
	d, err := ydb.Open(ctx, fmt.Sprintf("grpc://%s:%s/%s", host, port, databaseName))
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
	dktest.Run(t, image, opts, func(t *testing.T, c dktest.ContainerInfo) {
		db := &YDB{}
		d, err := db.Open(connectionString())
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
	dktest.Run(t, image, opts, func(t *testing.T, c dktest.ContainerInfo) {
		db := &YDB{}
		d, err := db.Open(connectionString())
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
