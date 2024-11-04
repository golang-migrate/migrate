package ydb

import (
	"testing"
	"time"

	"github.com/dhui/dktest"
	"github.com/docker/go-connections/nat"

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
					HostIP:   "0.0.0.0",
					HostPort: "2136",
				},
			},
		},

		Hostname:     "localhost",
		ReadyTimeout: 15 * time.Second,
	}
)

func Test(t *testing.T) {
	dktest.Run(t, image, opts, func(t *testing.T, c dktest.ContainerInfo) {
		ydb := &YDB{}

		d, err := ydb.Open("ydb://localhost:2136/local")
		if err != nil {
			t.Fatal(err)
		}

		defer func() {
			err := d.Close()
			if err != nil {
				t.Fatal(err)
			}
		}()

		dt.Test(t, d, []byte("CREATE TABLE `nested/a/b/c/table` (x Uint64 NOT NULL, PRIMARY KEY (x))"))
	})
}

func TestMigrate(t *testing.T) {
	dktest.Run(t, image, opts, func(t *testing.T, c dktest.ContainerInfo) {
		ydb := &YDB{}

		d, err := ydb.Open("ydb://localhost:2136/local")
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
