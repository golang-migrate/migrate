package ydb

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/docker/go-connections/nat"
	ydb "github.com/ydb-platform/ydb-go-sdk/v3"

	"github.com/dhui/dktest"
	dt "github.com/golang-migrate/migrate/v4/database/testing"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

const (
	host          = "localhost"
	port          = "2136"
	testDB        = "local"
	dbPingTimeout = 5 * time.Second
)

var (
	opts = dktest.Options{
		ReadyTimeout: 15 * time.Second,
		Hostname:     host,
		Env: map[string]string{
			"YDB_USE_IN_MEMORY_PDISKS": "true",
		},
		PortBindings: nat.PortMap{
			nat.Port(fmt.Sprintf("%s/tcp", port)): []nat.PortBinding{{
				HostIP:   "0.0.0.0",
				HostPort: port,
			}},
		},
		ReadyFunc: isReady,
	}

	image = "cr.yandex/yc/yandex-docker-local-ydb:latest"
)

func init() {
	_ = os.Setenv("YDB_ANONYMOUS_CREDENTIALS", "1")
}

func isReady(ctx context.Context, c dktest.ContainerInfo) bool {
	nativeDriver, err := ydb.Open(context.Background(), fmt.Sprintf("grpc://localhost:%s/%s", port, testDB))
	if err != nil {
		log.Println(err)
		return false
	}

	connector, err := ydb.Connector(nativeDriver)
	if err != nil {
		log.Println("close error:", err)
		return false
	}

	db := sql.OpenDB(connector)
	defer func() {
		if err := db.Close(); err != nil {
			log.Println("close error:", err)
		}
	}()

	ctxWithTimeout, cancel := context.WithTimeout(ctx, dbPingTimeout)
	defer cancel()

	if err = db.PingContext(ctxWithTimeout); err != nil {
		log.Println(err)
		return false
	}

	ctxWithTimeout = ydb.WithQueryMode(ctxWithTimeout, ydb.SchemeQueryMode)

	_, err = db.ExecContext(ctxWithTimeout, `
	CREATE TABLE test (
		id Int,
		PRIMARY KEY(id)
	);
	DROP TABLE test;`)
	if err != nil {
		log.Println(err)
		return false
	}

	return true
}

func Test(t *testing.T) {
	dktest.Run(t, image, opts, func(t *testing.T, c dktest.ContainerInfo) {
		addr := fmt.Sprintf("grpc://localhost:%s/%s", port, testDB)
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
		dt.Test(t, d, []byte(`
		CREATE TABLE test (
			id Int,
			PRIMARY KEY(id)
		);
		DROP TABLE test;`))
	})
}
