package duckdb

import (
	"fmt"
	"path/filepath"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/golang-migrate/migrate/v4"
	dt "github.com/golang-migrate/migrate/v4/database/testing"
	_ "github.com/golang-migrate/migrate/v4/source/file"
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

func TestNoTxWrap(t *testing.T) {
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "test.duckdb")
	addr := fmt.Sprintf("duckdb://%s?x-no-tx-wrap=true", dbFile)

	ddb := &DuckDB{}
	d, err := ddb.Open(addr)
	if err != nil {
		t.Fatalf("calling Open() on addr %s: %s", addr, err)
	}

	dt.Test(t, d, []byte("BEGIN TRANSACTION; CREATE TABLE t (Qty int, Name string); COMMIT;"))
}

func TestNoTxWrapInvalidValue(t *testing.T) {
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "test.duckdb")
	addr := fmt.Sprintf("duckdb://%s?x-no-tx-wrap=definitely", dbFile)

	ddb := &DuckDB{}
	_, err := ddb.Open(addr)
	if err == nil {
		t.Fatal("expected error for invalid x-no-tx-wrap value")
	}
}
