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
	image = "cr.yandex/yc/yandex-docker-local-ydb:latest"
	host  = "localhost"
	port  = "2136"
)

var (
	opts = dktest.Options{
		Env: map[string]string{
			"YDB_USE_IN_MEMORY_PDISKS":  "true",
			"YDB_LOCAL_SURVIVE_RESTART": "true",
			"GRPC_PORT":                 port,
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
	return fmt.Sprintf("grpc://%s:%s/local?%s", host, port, strings.Join(options, "&"))
}

func isReady(ctx context.Context, c dktest.ContainerInfo) bool {
	fmt.Println("1 connect")
	d, err := ydb.Open(ctx, connectionString())
	if err != nil {
		fmt.Println("2 err connect", err)
		return false
	}
	defer func() { _ = d.Close(ctx) }()

	fmt.Println("3 execute")
	res, err := d.Scripting().Execute(ctx, "SELECT 1", nil)
	if err != nil {
		fmt.Println("4 err execute", err)
		return false
	}
	defer func() { _ = res.Close() }()
	return true
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
