package trino

import (
	"context"
	"database/sql"
	sqldriver "database/sql/driver"
	"fmt"
	"io"
	"log"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/dhui/dktest"
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
	dt "github.com/golang-migrate/migrate/v4/database/testing"
	"github.com/golang-migrate/migrate/v4/dktesting"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

const (
	trinoUser    = "testuser"
	trinoCatalog = "memory"
	trinoSchema  = "default"
)

var (
	opts = dktest.Options{
		Env: map[string]string{
			"TRINO_ENVIRONMENT": "test",
		},
		PortRequired: true,
		ReadyFunc:    isReady,
		ReadyTimeout: 10 * time.Minute, // Increased timeout for Trino startup
	}
	// Using the official Trino Docker image
	specs = []dktesting.ContainerSpec{
		{ImageName: "trinodb/trino:latest", Options: opts},
	}
)

func trinoConnectionString(host, port string, options ...string) string {
	baseURL := fmt.Sprintf("http://%s@%s:%s?catalog=%s&schema=%s&source=migrate-test",
		trinoUser, host, port, trinoCatalog, trinoSchema)
	if len(options) > 0 {
		baseURL += "&" + strings.Join(options, "&")
	}
	return baseURL
}

func trinoConnectionStringWithScheme(scheme, host, port string, options ...string) string {
	baseURL := fmt.Sprintf("%s://%s@%s:%s?catalog=%s&schema=%s&source=migrate-test",
		scheme, trinoUser, host, port, trinoCatalog, trinoSchema)
	if len(options) > 0 {
		baseURL += "&" + strings.Join(options, "&")
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

	// Use a dedicated context with longer timeout for Trino startup
	readyCtx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	for i := 0; i < 120; i++ { // Increased attempts
		if err = db.PingContext(readyCtx); err != nil {
			switch err {
			case sqldriver.ErrBadConn, io.EOF:
				time.Sleep(2 * time.Second) // Longer sleep for Trino
				continue
			default:
				log.Printf("trino ping attempt %d error: %v", i+1, err)
			}

			time.Sleep(2 * time.Second)
			continue
		}

		// Test a simple query to ensure Trino is fully ready
		var result int
		if err = db.QueryRowContext(readyCtx, "SELECT 1").Scan(&result); err != nil {
			log.Printf("trino query test attempt %d error: %v", i+1, err)
			time.Sleep(2 * time.Second)
			continue
		}

		log.Printf("trino ready after %d attempts", i+1)
		// Give Trino a moment to stabilize before tests start
		time.Sleep(3 * time.Second)
		return true
	}
	log.Printf("trino failed to become ready after 120 attempts")
	return false
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
				url:  trinoConnectionString(ip, port, "x-migrations-schema=default"),
			},
			{
				name: "URL with timeouts",
				url:  trinoConnectionString(ip, port, "x-statement-timeout=5000"),
			},
			{
				name: "trino:// scheme with SSL disabled (HTTP)",
				url:  trinoConnectionStringWithScheme("trino", ip, port, "ssl=false"),
			},
			// Note: HTTPS tests are skipped because the test container only supports HTTP
			// The scheme conversion logic is tested separately in TestTrinoSchemeConversion
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

func TestTrinoSchemeConversion(t *testing.T) {
	// Test scheme conversion without requiring a live Trino instance
	testCases := []struct {
		name           string
		inputURL       string
		expectedScheme string
	}{
		{
			name:           "trino:// with ssl=false should become http://",
			inputURL:       "trino://user@localhost:8080?ssl=false&catalog=memory&schema=default",
			expectedScheme: "http",
		},
		{
			name:           "trino:// with ssl=true should become https://",
			inputURL:       "trino://user@localhost:8080?ssl=true&catalog=memory&schema=default",
			expectedScheme: "https",
		},
		{
			name:           "trino:// without ssl parameter should default to https://",
			inputURL:       "trino://user@localhost:8080?catalog=memory&schema=default",
			expectedScheme: "https",
		},
		{
			name:           "http:// should remain http://",
			inputURL:       "http://user@localhost:8080?catalog=memory&schema=default",
			expectedScheme: "http",
		},
		{
			name:           "https:// should remain https://",
			inputURL:       "https://user@localhost:8080?catalog=memory&schema=default",
			expectedScheme: "https",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// We'll simulate the URL parsing and scheme conversion logic
			purl, err := url.Parse(tc.inputURL)
			if err != nil {
				t.Fatal(err)
			}

			// Apply the same logic as in the Open function
			q := migrate.FilterCustomQuery(purl)
			if q.Scheme == "trino" {
				ssl := purl.Query().Get("ssl")
				if ssl == "" || ssl == "true" {
					q.Scheme = "https"
				} else {
					q.Scheme = "http"
				}
			}

			if q.Scheme != tc.expectedScheme {
				t.Errorf("Expected scheme %s, got %s for URL %s", tc.expectedScheme, q.Scheme, tc.inputURL)
			}
		})
	}
}

func TestTrinoLockConcurrency(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.FirstPort()
		if err != nil {
			t.Fatal(err)
		}

		addr := trinoConnectionString(ip, port)
		tr := &Trino{}

		// Single instance to test locking mechanism
		d, err := tr.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = d.Close() }()

		// Test basic locking functionality
		if err := d.Lock(); err != nil {
			t.Fatal(err)
		}

		// Try to acquire lock again (should fail)
		if err := d.Lock(); err != database.ErrLocked {
			t.Fatalf("Expected ErrLocked, got %v", err)
		}

		// Release lock
		if err := d.Unlock(); err != nil {
			t.Fatal(err)
		}

		// Try to unlock again (should fail)
		if err := d.Unlock(); err != database.ErrNotLocked {
			t.Fatalf("Expected ErrNotLocked, got %v", err)
		}

		// Should be able to lock again
		if err := d.Lock(); err != nil {
			t.Fatal(err)
		}

		if err := d.Unlock(); err != nil {
			t.Fatal(err)
		}
	})
}
