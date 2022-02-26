package ydb

// error codes https://github.com/lib/pq/blob/master/error.go

import (
	"context"
	"database/sql"
	sqldriver "database/sql/driver"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/docker/docker/api/types/mount"
	"github.com/docker/go-connections/nat"
	"github.com/golang-migrate/migrate/v4"

	"github.com/dhui/dktest"
	dt "github.com/golang-migrate/migrate/v4/database/testing"
	_ "github.com/golang-migrate/migrate/v4/source/file"

	ydbsql "github.com/ydb-platform/ydb-go-sql"
)

var (
	certsDirectory = "/tmp/ydb_certs"

	opts = dktest.Options{
		ReadyTimeout: 15 * time.Second,
		Hostname:     "localhost",
		Env: map[string]string{
			"YDB_USE_IN_MEMORY_PDISKS": "true",
		},
		PortBindings: nat.PortMap{
			nat.Port("2135/tcp"): []nat.PortBinding{{
				HostIP:   "0.0.0.0",
				HostPort: "2135",
			}},
		},
		ReadyFunc: isReady,
		Mounts: []mount.Mount{
			{
				Type:   mount.TypeBind,
				Source: certsDirectory,
				Target: "/ydb_certs",
			},
		},
	}

	image = "cr.yandex/yc/yandex-docker-local-ydb:latest"
)

func init() {
	_ = os.MkdirAll(certsDirectory, os.ModePerm)
	_ = os.Setenv("YDB_SSL_ROOT_CERTIFICATES_FILE", path.Join(certsDirectory, "ca.pem"))
	_ = os.Setenv("YDB_ANONYMOUS_CREDENTIALS", "1")
}

func ydbConnectionString(options ...string) string {
	return fmt.Sprintf("grpcs://localhost:2135/?%s", strings.Join(options, "&"))
}

func isReady(ctx context.Context, c dktest.ContainerInfo) bool {
	db, err := sql.Open("ydb", ydbConnectionString("localhost", "2135", "database=/local", "x-use-grpcs-scheme"))
	if err != nil {
		return false
	}
	defer func() {
		if err := db.Close(); err != nil {
			log.Println("close error:", err)
		}
	}()
	if err = db.PingContext(ctx); err != nil {
		switch err {
		case sqldriver.ErrBadConn, io.EOF:
			return false
		default:
			log.Println(err)
		}
		return false
	}

	// Wait for container to bootup
	time.Sleep(10 * time.Second)

	return true
}

func Test(t *testing.T) {
	dktest.Run(t, image, opts, func(t *testing.T, c dktest.ContainerInfo) {
		addr := ydbConnectionString("localhost", "2135", "database=/local", "x-use-grpcs-scheme")
		p := &YDB{}
		d, err := p.Open(addr)
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

func TestMigrate(t *testing.T) {
	dktest.Run(t, image, opts, func(t *testing.T, c dktest.ContainerInfo) {
		addr := ydbConnectionString("localhost", "2135", "database=/local", "x-use-grpcs-scheme")
		p := &YDB{}
		d, err := p.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()
		m, err := migrate.NewWithDatabaseInstance("file://./examples/migrations", "ydb", d)
		if err != nil {
			t.Fatal(err)
		}
		dt.TestMigrate(t, m)
	})
}

func TestMultipleStatements(t *testing.T) {
	dktest.Run(t, image, opts, func(t *testing.T, c dktest.ContainerInfo) {
		addr := ydbConnectionString("localhost", "2135", "database=/local", "x-use-grpcs-scheme")
		p := &YDB{}
		d, err := p.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()
		if err := d.Run(strings.NewReader("CREATE TABLE foo (foo Utf8, PRIMARY KEY(foo)); CREATE TABLE bar (bar Utf8, PRIMARY KEY(bar));")); err != nil {
			t.Fatalf("expected err to be nil, got %v", err)
		}

		// make sure second table exists
		var table string
		if err := d.(*YDB).conn.QueryRowContext(ydbsql.WithScanQuery(context.Background()), "SELECT DISTINCT Path FROM `.sys/partition_stats` WHERE Path LIKE 'bar'").Scan(&table); err != sql.ErrNoRows {
			t.Fatalf("expected table bar to exist")
		}
	})
}
