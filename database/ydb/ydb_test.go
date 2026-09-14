package ydb

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dhui/dktest"
	"github.com/docker/go-connections/nat"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/balancers"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
	dt "github.com/golang-migrate/migrate/v4/database/testing"
	"github.com/golang-migrate/migrate/v4/dktesting"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

const databaseName = "local"

var (
	getOptions = func(port string) dktest.Options {
		return dktest.Options{
			Env: map[string]string{
				"GRPC_TLS_PORT": "2135",
				"GRPC_PORT":     port,
				"MON_PORT":      "8765",
			},
			PortBindings: nat.PortMap{
				nat.Port(port + "/tcp"): []nat.PortBinding{
					{
						HostIP:   "0.0.0.0",
						HostPort: port,
					},
				},
			},
			ExposedPorts: nat.PortSet{nat.Port(port + "/tcp"): {}},
			PortRequired: true,
			Hostname:     "127.0.0.1",
			ReadyTimeout: 15 * time.Second,
			ReadyFunc:    isReady,
		}
	}

	// Released version: https://ydb.tech/docs/downloads/#ydb-server
	specs = []dktesting.ContainerSpec{
		{ImageName: "ydbplatform/local-ydb:latest", Options: getOptions("22000")},
		{ImageName: "ydbplatform/local-ydb:26.1", Options: getOptions("22001")},
	}
)

func containerAddress(c dktest.ContainerInfo) (string, string, error) {
	for _, port := range []uint16{22000, 22001} {
		host, mapped, err := c.Port(port)
		if err == nil {
			return host, mapped, nil
		}
	}
	return "", "", fmt.Errorf("YDB gRPC port not published")
}

func isReady(ctx context.Context, c dktest.ContainerInfo) bool {
	ip, port, err := containerAddress(c)
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

// ydb-test-dsn opts into an existing server instead of creating a Docker
// database. The endpoint must be disposable: tests delete all its objects.
var testDSN = flag.String("ydb-test-dsn", "", "existing YDB endpoint, e.g. grpc://localhost:2136/local")

func withServer(t *testing.T, test func(*testing.T, string)) {
	t.Helper()
	if *testDSN != "" {
		test(t, *testDSN)
		return
	}
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := containerAddress(c)
		if err != nil {
			t.Fatal(err)
		}
		t.Run("driver", func(t *testing.T) {
			test(t, fmt.Sprintf("grpc://%s:%s/%s", ip, port, databaseName))
		})
	})
}

func nativeClient(t *testing.T, dsn string) *ydb.Driver {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client, err := ydb.Open(ctx, dsn, ydb.WithAnonymousCredentials(), ydb.WithBalancer(balancers.SingleConn()))
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func testSchema(t *testing.T, dsn string) string {
	t.Helper()
	client := nativeClient(t, dsn)
	schema := client.Name()
	d, err := WithInstance(client, &Config{})
	if err != nil {
		_ = client.Close(context.Background())
		t.Fatal(err)
	}
	if err := d.Drop(); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		client := nativeClient(t, dsn)
		d, err := WithInstance(client, &Config{})
		if err != nil {
			_ = client.Close(context.Background())
			t.Error(err)
			return
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()
		if err := d.Drop(); err != nil {
			t.Error(err)
		}
	})
	return schema
}

func openDriver(t *testing.T, dsn string) database.Driver {
	t.Helper()
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	u.Scheme = "ydb"
	d, err := (&YDB{}).Open(u.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := d.Close(); err != nil {
			t.Error(err)
		}
	})
	return d
}

func Test(t *testing.T) {
	withServer(t, func(t *testing.T, dsn string) {
		testSchema(t, dsn)
		d := openDriver(t, dsn)
		native := d.(*YDB)
		timeout := native.config.StatementTimeout
		native.config.StatementTimeout = time.Nanosecond
		err := d.Run(bytes.NewBufferString("SELECT 1"))
		native.config.StatementTimeout = timeout
		var queryErr *database.Error
		if !errors.Is(err, context.DeadlineExceeded) && (!errors.As(err, &queryErr) || !errors.Is(queryErr.OrigErr, context.DeadlineExceeded)) {
			t.Fatalf("expired statement deadline: %v", err)
		}
		badSQL := "THIS IS NOT VALID YQL"
		err = d.Run(bytes.NewBufferString(badSQL))
		if !errors.As(err, &queryErr) || string(queryErr.Query) != badSQL || queryErr.OrigErr == nil {
			t.Fatalf("migration error lost query or cause: %v", err)
		}
		dt.Test(t, d, []byte("CREATE TABLE `a/b/c/d/table` (x Uint64 NOT NULL, PRIMARY KEY (x))"))
	})
}

func TestMigrate(t *testing.T) {
	withServer(t, func(t *testing.T, dsn string) {
		schema := testSchema(t, dsn)
		// Reopening and applying the same migrations proves Drop removed non-table
		// objects too, including the topic and nested directories in the fixtures.
		for i := 0; i < 2; i++ {
			d := openDriver(t, dsn)
			m, err := migrate.NewWithDatabaseInstance("file://./examples/migrations", "ydb", d)
			if err != nil {
				t.Fatal(err)
			}
			if err := m.Up(); err != nil {
				t.Fatal(err)
			}
			if err := m.Down(); err != nil {
				t.Fatal(err)
			}
			dt.TestMigrate(t, m)
		}
		client := nativeClient(t, dsn)
		defer func() { _ = client.Close(context.Background()) }()
		listing, err := client.Scheme().ListDirectory(context.Background(), schema)
		if err != nil {
			t.Fatal(err)
		}
		for _, child := range listing.Children {
			if child.Name != systemDirectory && child.Name != systemMetadataDirectory {
				t.Fatalf("Drop left object: %+v", child)
			}
		}
	})
}

func TestIndependentClientsLock(t *testing.T) {
	withServer(t, func(t *testing.T, dsn string) {
		testSchema(t, dsn)
		drivers := make([]database.Driver, 2)
		for i := range drivers {
			client := nativeClient(t, dsn)
			d, err := WithInstance(client, &Config{})
			if err != nil {
				_ = client.Close(context.Background())
				t.Fatal(err)
			}
			drivers[i] = d
			t.Cleanup(func() {
				if err := d.Close(); err != nil {
					t.Error(err)
				}
			})
		}
		for round := 0; round < 5; round++ {
			start := make(chan struct{})
			results := make([]error, len(drivers))
			var wg sync.WaitGroup
			for i, d := range drivers {
				wg.Add(1)
				go func(i int, d database.Driver) { defer wg.Done(); <-start; results[i] = d.Lock() }(i, d)
			}
			close(start)
			wg.Wait()
			winner := -1
			for i, err := range results {
				if err == nil {
					if winner != -1 {
						t.Fatal("both independent clients acquired the migration lock")
					}
					winner = i
				} else if !errors.Is(err, database.ErrLocked) {
					t.Fatalf("Lock: %v", err)
				}
			}
			if winner == -1 {
				t.Fatalf("neither client acquired lock: %v", results)
			}
			if err := drivers[1-winner].Unlock(); !errors.Is(err, database.ErrNotLocked) {
				t.Fatalf("non-owner Unlock: %v", err)
			}
			if err := drivers[winner].Unlock(); err != nil {
				t.Fatal(err)
			}
			if err := drivers[1-winner].Lock(); err != nil {
				t.Fatalf("lock after release: %v", err)
			}
			if err := drivers[1-winner].Unlock(); err != nil {
				t.Fatal(err)
			}
		}
	})
}

func TestWithInstanceInvalidConfigRetainsClient(t *testing.T) {
	withServer(t, func(t *testing.T, dsn string) {
		client := nativeClient(t, dsn)
		defer func() { _ = client.Close(context.Background()) }()
		if _, err := WithInstance(client, &Config{StatementTimeout: -time.Second}); err == nil {
			t.Fatal("negative statement timeout accepted")
		}
		if _, err := WithInstance(client, nil); !errors.Is(err, ErrNilConfig) {
			t.Fatalf("WithInstance(nil config): %v", err)
		}
		if _, err := client.Scheme().DescribePath(context.Background(), client.Name()); err != nil {
			t.Fatalf("constructor closed caller's client: %v", err)
		}
	})
}

func TestWithInstanceInitializationFailureRetainsClient(t *testing.T) {
	withServer(t, func(t *testing.T, dsn string) {
		testSchema(t, dsn)
		client := nativeClient(t, dsn)
		defer func() { _ = client.Close(context.Background()) }()
		if err := client.Query().Exec(context.Background(), "CREATE TOPIC `schema_migrations`"); err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := client.Topic().Drop(context.Background(), client.Name()+"/schema_migrations"); err != nil {
				t.Error(err)
			}
		}()
		if _, err := WithInstance(client, &Config{}); err == nil {
			t.Fatal("expected initialization failure for a topic in place of the version table")
		}
		if _, err := client.Scheme().DescribePath(context.Background(), client.Name()); err != nil {
			t.Fatalf("constructor closed caller's client: %v", err)
		}
	})
}

func TestParseBoolOption(t *testing.T) {
	for _, name := range []string{queryParamUseGRPCS, queryParamTLSInsecureSkipVerify} {
		for _, tc := range []struct {
			name, value            string
			present, want, wantErr bool
		}{
			{name: "absent"},
			{name: "bare", present: true, want: true},
			{name: "true", value: "true", present: true, want: true},
			{name: "false", value: "false", present: true},
			{name: "invalid", value: "invalid", present: true, wantErr: true},
		} {
			t.Run(name+"/"+tc.name, func(t *testing.T) {
				values := make(url.Values)
				if tc.present {
					values.Set(name, tc.value)
				}
				got, err := parseBoolOption(values, name)
				if (err != nil) != tc.wantErr || got != tc.want {
					t.Fatalf("got (%t, %v), want (%t, error=%t)", got, err, tc.want, tc.wantErr)
				}
			})
		}
	}
}

func TestInvalidStatementTimeout(t *testing.T) {
	for _, value := range []string{"", "0", "-1", "invalid", "9223372036854775807"} {
		t.Run(value, func(t *testing.T) {
			if _, err := (&YDB{}).Open("ydb://localhost:1/local?x-statement-timeout=" + value); err == nil {
				t.Fatal("invalid timeout accepted")
			}
		})
	}
}

// Run wraps session execution errors in database.Error so arbitrary migration
// text is not retried. Guard that SDK retry contract if Error gains Unwrap.
func TestMigrationErrorStopsSDKRetry(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	executionErr := retry.RetryableError(errors.New("temporary execution failure"))
	wrapped := &database.Error{OrigErr: executionErr, Query: []byte("INSERT INTO items VALUES (1)")}
	attempts := 0
	err := retry.Retry(ctx, func(context.Context) error { attempts++; return wrapped }, retry.WithIdempotent(true))
	if attempts != 1 {
		t.Fatalf("migration execution error retried %d times", attempts)
	}
	var got *database.Error
	if !errors.As(err, &got) || got != wrapped {
		t.Fatalf("lost original migration error: %v", err)
	}
}

func TestDropExternalObjects(t *testing.T) {
	withServer(t, func(t *testing.T, dsn string) {
		schema := testSchema(t, dsn)
		d := openDriver(t, dsn)
		// Source sorts before its dependent table in directory order. Drop must
		// remove the table first even when the objects live in separate directories.

		sourceSQL := `CREATE EXTERNAL DATA SOURCE ` + "`a_sources/source`" + ` WITH (
   SOURCE_TYPE="ObjectStorage", LOCATION="http://localhost:1/bucket", AUTH_METHOD="NONE"
  );`
		if err := d.Run(bytes.NewBufferString(sourceSQL)); err != nil {
			if strings.Contains(err.Error(), "External data sources are disabled. Please contact your system administrator to enable it") {
				t.Skip("server disables external data sources; external-object Drop is not tested on this image")
			}
			t.Fatal(err)
		}
		tableSQL := `CREATE EXTERNAL TABLE ` + "`z_tables/table`" + ` (key Utf8 NOT NULL) WITH (
   DATA_SOURCE="` + schema + `/a_sources/source", LOCATION="/", FORMAT="csv_with_names"
  );`
		if err := d.Run(bytes.NewBufferString(tableSQL)); err != nil {
			t.Fatal(err)
		}

		if err := d.Drop(); err != nil {
			t.Fatal(err)
		}
		client := nativeClient(t, dsn)
		defer func() { _ = client.Close(context.Background()) }()
		listing, err := client.Scheme().ListDirectory(context.Background(), schema)
		if err != nil {
			t.Fatal(err)
		}
		for _, child := range listing.Children {
			if child.Name != systemDirectory && child.Name != systemMetadataDirectory {
				t.Fatalf("Drop left object: %+v", child)
			}
		}
	})
}
