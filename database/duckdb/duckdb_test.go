package duckdb

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"

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

	db, err := sql.Open("duckdb", dbFile)
	if err != nil {
		t.Fatalf("sql open: %s", err)
	}
	defer func() {
		assert.NoError(t, db.Close())
	}()

	driver, err := WithInstance(db, &Config{})
	if err != nil {
		t.Fatalf("with instance: %s", err)
	}

	m, err := migrate.NewWithDatabaseInstance(
		"file://./examples/migrations",
		"main",
		driver)
	if err != nil {
		t.Fatalf("new migrate: %s", err)
	}

	dt.TestMigrate(t, m)
}

func TestMigrationTable(t *testing.T) {
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "test.duckdb")

	db, err := sql.Open("duckdb", dbFile)
	if err != nil {
		t.Fatalf("sql open: %s", err)
	}
	defer func() {
		assert.NoError(t, db.Close())
	}()

	config := &Config{
		MigrationsTable: "custom_migrations",
	}
	driver, err := WithInstance(db, config)
	if err != nil {
		t.Fatalf("with instance: %s", err)
	}

	m, err := migrate.NewWithDatabaseInstance(
		"file://./examples/migrations",
		"main",
		driver)
	if err != nil {
		t.Fatalf("new migrate: %s", err)
	}

	if err := m.Up(); err != nil {
		t.Fatalf("up: %s", err)
	}

	if _, err := db.Query(fmt.Sprintf("SELECT * FROM %s", config.MigrationsTable)); err != nil {
		t.Fatalf("query migrations table: %s", err)
	}
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
	if assert.Error(t, err) {
		assert.Contains(t, err.Error(), "x-no-tx-wrap")
		assert.Contains(t, err.Error(), "invalid syntax")
	}
}
