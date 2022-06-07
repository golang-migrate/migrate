package immudb

import (
	"context"
	sqldriver "database/sql/driver"
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"
	"testing"

	"github.com/golang-migrate/migrate/v4"

	immudb "github.com/codenotary/immudb/pkg/client"
	_ "github.com/codenotary/immudb/pkg/stdlib"

	"github.com/dhui/dktest"
	dt "github.com/golang-migrate/migrate/v4/database/testing"
	"github.com/golang-migrate/migrate/v4/dktesting"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

const (
	defaultPort      = 3322
	testDatabaseName = "migrationtest"
)

var (
	opts  = dktest.Options{PortRequired: true, ReadyFunc: isReady}
	specs = []dktesting.ContainerSpec{
		{ImageName: "codenotary/immudb:1.3", Options: opts},
	}
	defaultUser = []byte("immudb")
	defaultPass = []byte("immudb")
)

func immudbConnectionString(host, port string, options ...string) string {
	options = append(options, "sslmode=disable")
	return fmt.Sprintf(
		"immudb://%s:%s@%s:%s/%s?%s",
		defaultUser,
		defaultPass,
		host,
		port,
		testDatabaseName,
		strings.Join(options, "&"),
	)
}

func createClient(ctx context.Context, ip string, port int, databaseName string) (immudb.ImmuClient, error) {
	options := immudb.DefaultOptions().WithAddress(ip).WithPort(port)
	client := immudb.NewClient().WithOptions(options)
	err := client.OpenSession(ctx, defaultUser, defaultPass, databaseName)
	if err != nil {
		return nil, err
	}
	return client, nil
}

func isReady(ctx context.Context, c dktest.ContainerInfo) bool {
	ip, aport, err := c.Port(defaultPort)
	if err != nil {
		return false
	}
	port, err := strconv.Atoi(aport)
	if err != nil {
		return false
	}

	client, err := createClient(ctx, ip, port, "defaultdb")
	if err != nil {
		return false
	}
	_, err = client.CreateDatabaseV2(ctx, testDatabaseName, nil)
	if err != nil {
		return false
	}
	err = client.CloseSession(ctx)
	if err != nil {
		return false
	}

	client, err = createClient(ctx, ip, port, testDatabaseName)
	if err != nil {
		return false
	}

	defer func() {
		if err := client.CloseSession(ctx); err != nil {
			log.Println("close error:", err)
		}
	}()
	if err = client.HealthCheck(ctx); err != nil {
		switch err {
		case sqldriver.ErrBadConn, io.EOF:
			return false
		default:
			log.Println(err)
		}
		return false
	}

	return true
}

func Test(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.FirstPort()
		if err != nil {
			t.Fatal(err)
		}

		addr := immudbConnectionString(ip, port)
		i := &Immudb{}
		d, err := i.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()
		dt.Test(t, d, []byte("CREATE TABLE t (id INTEGER, name VARCHAR, PRIMARY KEY id);"))
	})
}

func TestMigrate(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.FirstPort()
		if err != nil {
			t.Fatal(err)
		}

		addr := immudbConnectionString(ip, port)
		i := &Immudb{}
		d, err := i.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()
		m, err := migrate.NewWithDatabaseInstance("file://./examples/migrations", testDatabaseName, d)
		if err != nil {
			t.Fatal(err)
		}
		dt.TestMigrate(t, m)
	})
}

func TestMultipleStatements(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.FirstPort()
		if err != nil {
			t.Fatal(err)
		}

		addr := immudbConnectionString(ip, port)
		i := &Immudb{}
		d, err := i.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()
		multiLineStatement := `CREATE TABLE first (id INTEGER, name VARCHAR, PRIMARY KEY id); 
	CREATE TABLE second (id INTEGER, name VARCHAR, PRIMARY KEY id);`
		if err := d.Run(strings.NewReader(multiLineStatement)); err != nil {
			t.Fatalf("expected err to be nil, got %v", err)
		}

		// make sure the second table exists
		if _, err := d.(*Immudb).client.SQLQuery(context.Background(), "SELECT id FROM second", nil, true); err != nil {
			t.Fatal(err)
		}
	})
}
