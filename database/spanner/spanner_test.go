package spanner

import (
	"fmt"
	"os"
	"testing"

	"github.com/golang-migrate/migrate/v4"

	dt "github.com/golang-migrate/migrate/v4/database/testing"
	_ "github.com/golang-migrate/migrate/v4/source/file"

	"cloud.google.com/go/spanner/spannertest"
)

// withSpannerEmulator is not thread-safe and cannot be used with parallel tests since it sets the emulator
func withSpannerEmulator(t *testing.T, testFunc func(t *testing.T)) {
	t.Helper()
	srv, err := spannertest.NewServer("localhost:0")
	if err != nil {
		t.Fatal("Failed to create Spanner emulator:", err)
	}
	// This is not thread-safe
	if err := os.Setenv("SPANNER_EMULATOR_HOST", srv.Addr); err != nil {
		t.Fatal("Failed to set SPANNER_EMULATOR_HOST env var:", err)
	}
	defer srv.Close()
	testFunc(t)

}

const db = "projects/abc/instances/def/databases/testdb"

func Test(t *testing.T) {
	withSpannerEmulator(t, func(t *testing.T) {
		uri := fmt.Sprintf("spanner://%s", db)
		s := &Spanner{}
		d, err := s.Open(uri)
		if err != nil {
			t.Fatal(err)
		}
		dt.Test(t, d, []byte("CREATE TABLE test (id BOOL) PRIMARY KEY (id)"))
	})
}

func TestMigrate(t *testing.T) {
	withSpannerEmulator(t, func(t *testing.T) {
		s := &Spanner{}
		uri := fmt.Sprintf("spanner://%s", db)
		d, err := s.Open(uri)
		if err != nil {
			t.Fatal(err)
		}
		m, err := migrate.NewWithDatabaseInstance("file://./examples/migrations", uri, d)
		if err != nil {
			t.Fatal(err)
		}
		dt.TestMigrate(t, m)
	})
}
