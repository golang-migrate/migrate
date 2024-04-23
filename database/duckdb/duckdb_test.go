package duckdb

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/golang-migrate/migrate/v4"
	dt "github.com/golang-migrate/migrate/v4/database/testing"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	_ "github.com/marcboeker/go-duckdb"
)

func Test(t *testing.T) {
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "test.duckdb")
	addr := fmt.Sprintf("duckdb://%s", dbFile)

	ddb := &DuckDB{}
	d, err := ddb.Open(addr)
	if err != nil {
		t.Fatalf("calling Open() on addr %s: %s", addr, err)
	}

	dt.Test(t, d, []byte(`CREATE TABLE t (Qty int, Name string);`))
}

func TestMigrate(t *testing.T) {
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "test.duckdb")
	addr := fmt.Sprintf("duckdb://%s", dbFile)

	ddb := &DuckDB{}
	d, err := ddb.Open(addr)
	if err != nil {
		t.Fatalf("calling Open() on addr %s: %s", addr, err)
	}

	m, err := migrate.NewWithDatabaseInstance(
		"file://./examples/migrations",
		"main",
		d,
	)
	if err != nil {
		t.Fatalf("new migrate: %s", err)
	}

	dt.TestMigrate(t, m)
}
