package ydb

import (
	"context"
	"testing"
	"time"

	"github.com/dhui/dktest"
	"github.com/docker/go-connections/nat"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"

	"github.com/golang-migrate/migrate/v4"
	dt "github.com/golang-migrate/migrate/v4/database/testing"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

const (
	image = "cr.yandex/yc/yandex-docker-local-ydb:latest"
)

var (
	opts = dktest.Options{
		Env: map[string]string{
			"YDB_USE_IN_MEMORY_PDISKS": "true",
			"GRPC_PORT":                "2136",
		},

		PortBindings: nat.PortMap{
			nat.Port("2136/tcp"): []nat.PortBinding{
				{
					HostIP:   "localhost",
					HostPort: "2136",
				},
			},
		},

		Hostname: "localhost",
		Platform: "linux/amd64",

		ReadyTimeout: 15 * time.Second,
		ReadyFunc: func(ctx context.Context, c dktest.ContainerInfo) bool {
			d, err := ydb.Open(ctx, "grpc://localhost:2136/local")
			if err != nil {
				return false
			}
			defer d.Close(ctx)

			err = d.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
				return s.ExecuteSchemeQuery(ctx, "CREATE TOPIC ready")
			})

			return err == nil
		},
	}
)

func Test(t *testing.T) {
	dktest.Run(t, image, opts, func(t *testing.T, c dktest.ContainerInfo) {
		ydb := &YDB{}

		d, err := ydb.Open("ydb://localhost:2136/local?x-insecure=true&x-connect-timeout=5s")
		if err != nil {
			t.Fatal(err)
		}

		defer func() {
			err := d.Close()
			if err != nil {
				t.Fatal(err)
			}
		}()

		dt.Test(t, d, []byte("CREATE TABLE `Kek/test` (a Uint64 NOT NULL, PRIMARY KEY (a))"))
	})
}

func TestMigrate(t *testing.T) {
	dktest.Run(t, image, opts, func(t *testing.T, c dktest.ContainerInfo) {
		ydb := &YDB{}

		d, err := ydb.Open("ydb://localhost:2136/local?x-insecure=true&x-connect-timeout=5s")
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
