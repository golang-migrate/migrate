package trino

import (
	"context"
	"database/sql"
	sqldriver "database/sql/driver"
	"fmt"
	"io"
	"log"
	"strings"
	"testing"

	"github.com/dhui/dktest"
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
	dt "github.com/golang-migrate/migrate/v4/database/testing"
	"github.com/golang-migrate/migrate/v4/dktesting"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

const (
	trinoUser     = "testuser"
	trinoCatalog  = "memory"
	trinoSchema   = "default"
)

var (
	opts = dktest.Options{
		Env: map[string]string{
			"TRINO_ENVIRONMENT": "test",
		},
		PortRequired: true, 
		ReadyFunc:    isReady,
	}
	// Using the official Trino Docker image
	specs = []dktesting.ContainerSpec{
		{ImageName: "trinodb/trino:latest", Options: opts},
	}
)

func trinoConnectionString(host, port string, options ...string) string {
	baseURL := fmt.Sprintf("trino://%s@%s:%s/%s/%s", trinoUser, host, port, trinoCatalog, trinoSchema)
	if len(options) > 0 {
		baseURL += "?" + strings.Join(options, "&")
	}
	return baseURL
}

func isReady(ctx context.Context, c dktest.ContainerInfo) bool {
	ip, port, err := c.FirstPort()
	if err != nil {
		return false
	}

	// Build the direct Trino HTTP URL for sql.Open
	trinoURL := fmt.Sprintf("http://%s@%s:%s?catalog=%s&schema=%s&source=migrate-test", 
		trinoUser, ip, port, trinoCatalog, trinoSchema)
	
	db, err := sql.Open("trino", trinoURL)
	if err != nil {
		log.Printf("trino open error: %v", err)
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
			log.Printf("trino ping error: %v", err)
		}
		return false
	}

	return true
}

func mustRun(t *testing.T, d database.Driver, statements []string) {
	for _, statement := range statements {
		if err := d.Run(strings.NewReader(statement)); err != nil {
			t.Fatal(err)
		}
	}
}

func Test(t *testing.T) {
	t.Run("test", test)
	t.Run("testMigrate", testMigrate)
	t.Run("testLockingMethods", testLockingMethods)
	t.Run("testWithInstance", testWithInstance)
	t.Run("testOpen", testOpen)

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

		addr := trinoConnectionString(ip, port)
		tr := &Trino{}
		d, err := tr.Open(addr)
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

func testMigrate(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.FirstPort()
		if err != nil {
			t.Fatal(err)
		}

		addr := trinoConnectionString(ip, port)
		tr := &Trino{}
		d, err := tr.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()
		m, err := migrate.NewWithDatabaseInstance("file://./examples/migrations", "trino", d)
		if err != nil {
			t.Fatal(err)
		}
		dt.TestMigrate(t, m)
	})
}

func testLockingMethods(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.FirstPort()
		if err != nil {
			t.Fatal(err)
		}

		// Test file-based locking (default)
		addr := trinoConnectionString(ip, port, "x-lock-method=file")
		tr := &Trino{}
		d, err := tr.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()

		// Test locking functionality
		if err := d.Lock(); err != nil {
			t.Fatal(err)
		}
		if err := d.Unlock(); err != nil {
			t.Fatal(err)
		}

		// Test table-based locking
		addr2 := trinoConnectionString(ip, port, "x-lock-method=table")
		d2, err := tr.Open(addr2)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d2.Close(); err != nil {
				t.Error(err)
			}
		}()

		if err := d2.Lock(); err != nil {
			t.Fatal(err)
		}
		if err := d2.Unlock(); err != nil {
			t.Fatal(err)
		}

		// Test no locking
		addr3 := trinoConnectionString(ip, port, "x-lock-method=none")
		d3, err := tr.Open(addr3)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d3.Close(); err != nil {
				t.Error(err)
			}
		}()

		if err := d3.Lock(); err != nil {
			t.Fatal(err)
		}
		if err := d3.Unlock(); err != nil {
			t.Fatal(err)
		}
	})
}

func testWithInstance(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.FirstPort()
		if err != nil {
			t.Fatal(err)
		}

		// Create direct connection to Trino
		trinoURL := fmt.Sprintf("http://%s@%s:%s?catalog=%s&schema=%s&source=migrate-test", 
			trinoUser, ip, port, trinoCatalog, trinoSchema)
		
		db, err := sql.Open("trino", trinoURL)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := db.Close(); err != nil {
				t.Error(err)
			}
		}()

		config := &Config{
			MigrationsCatalog: trinoCatalog,
			MigrationsSchema:  trinoSchema,
			User:              trinoUser,
		}

		d, err := WithInstance(db, config)
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

func testOpen(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.FirstPort()
		if err != nil {
			t.Fatal(err)
		}

		// Test various URL formats
		testCases := []struct {
			name string
			url  string
		}{
			{
				name: "basic URL",
				url:  trinoConnectionString(ip, port),
			},
			{
				name: "URL with custom migrations table",
				url:  trinoConnectionString(ip, port, "x-migrations-table=custom_migrations"),
			},
			{
				name: "URL with custom schema",
				url:  trinoConnectionString(ip, port, "x-migrations-schema=test_schema"),
			},
			{
				name: "URL with timeouts",
				url:  trinoConnectionString(ip, port, "x-statement-timeout=5000", "x-connection-timeout=10000"),
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				tr := &Trino{}
				d, err := tr.Open(tc.url)
				if err != nil {
					t.Fatal(err)
				}
				defer func() {
					if err := d.Close(); err != nil {
						t.Error(err)
					}
				}()

				// Test basic functionality
				version, dirty, err := d.Version()
				if err != nil {
					t.Fatal(err)
				}
				if version != database.NilVersion {
					t.Fatalf("Expected NilVersion, got %v", version)
				}
				if dirty {
					t.Fatal("Expected clean state")
				}
			})
		}
	})
}

func TestTrinoLockConcurrency(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.FirstPort()
		if err != nil {
			t.Fatal(err)
		}

		addr := trinoConnectionString(ip, port, "x-lock-method=file")
		tr := &Trino{}
		
		// First instance
		d1, err := tr.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer d1.Close()

		// Second instance
		d2, err := tr.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer d2.Close()

		// First instance acquires lock
		if err := d1.Lock(); err != nil {
			t.Fatal(err)
		}

		// Second instance should fail to acquire lock
		if err := d2.Lock(); err != database.ErrLocked {
			t.Fatalf("Expected ErrLocked, got %v", err)
		}

		// First instance releases lock
		if err := d1.Unlock(); err != nil {
			t.Fatal(err)
		}

		// Second instance should now be able to acquire lock
		if err := d2.Lock(); err != nil {
			t.Fatal(err)
		}

		if err := d2.Unlock(); err != nil {
			t.Fatal(err)
		}
	})
}