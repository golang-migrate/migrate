package sqlite3

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/golang-migrate/migrate/v4"
	dt "github.com/golang-migrate/migrate/v4/database/testing"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	_ "github.com/mattn/go-sqlite3"
)

func Test(t *testing.T) {
	dir := t.TempDir()
	t.Logf("DB path : %s\n", filepath.Join(dir, "sqlite3.db"))
	p := &Sqlite{}
	addr := fmt.Sprintf("sqlite3://%s", filepath.Join(dir, "sqlite3.db"))
	d, err := p.Open(addr)
	if err != nil {
		t.Fatal(err)
	}
	dt.Test(t, d, []byte("CREATE TABLE t (Qty int, Name string);"))
}

func TestMigrate(t *testing.T) {
	dir := t.TempDir()
	t.Logf("DB path : %s\n", filepath.Join(dir, "sqlite3.db"))

	db, err := sql.Open("sqlite3", filepath.Join(dir, "sqlite3.db"))
	if err != nil {
		return
	}
	defer func() {
		if err := db.Close(); err != nil {
			return
		}
	}()
	driver, err := WithInstance(db, &Config{})
	if err != nil {
		t.Fatal(err)
	}

	m, err := migrate.NewWithDatabaseInstance(
		"file://./examples/migrations",
		"ql", driver)
	if err != nil {
		t.Fatal(err)
	}
	dt.TestMigrate(t, m)
}

func TestMigrationTable(t *testing.T) {
	dir := t.TempDir()

	t.Logf("DB path : %s\n", filepath.Join(dir, "sqlite3.db"))

	db, err := sql.Open("sqlite3", filepath.Join(dir, "sqlite3.db"))
	if err != nil {
		return
	}
	defer func() {
		if err := db.Close(); err != nil {
			return
		}
	}()

	config := &Config{
		MigrationsTable: "my_migration_table",
	}
	driver, err := WithInstance(db, config)
	if err != nil {
		t.Fatal(err)
	}
	m, err := migrate.NewWithDatabaseInstance(
		"file://./examples/migrations",
		"ql", driver)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("UP")
	err = m.Up()
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Query(fmt.Sprintf("SELECT * FROM %s", config.MigrationsTable))
	if err != nil {
		t.Fatal(err)
	}
}

func TestNoTxWrap(t *testing.T) {
	dir := t.TempDir()
	t.Logf("DB path : %s\n", filepath.Join(dir, "sqlite3.db"))
	p := &Sqlite{}
	addr := fmt.Sprintf("sqlite3://%s?x-no-tx-wrap=true", filepath.Join(dir, "sqlite3.db"))
	d, err := p.Open(addr)
	if err != nil {
		t.Fatal(err)
	}
	// An explicit BEGIN statement would ordinarily fail without x-no-tx-wrap.
	// (Transactions in sqlite may not be nested.)
	dt.Test(t, d, []byte("BEGIN; CREATE TABLE t (Qty int, Name string); COMMIT;"))
}

func TestNoTxWrapInvalidValue(t *testing.T) {
	dir := t.TempDir()
	t.Logf("DB path : %s\n", filepath.Join(dir, "sqlite3.db"))
	p := &Sqlite{}
	addr := fmt.Sprintf("sqlite3://%s?x-no-tx-wrap=yeppers", filepath.Join(dir, "sqlite3.db"))
	_, err := p.Open(addr)
	if assert.Error(t, err) {
		assert.Contains(t, err.Error(), "x-no-tx-wrap")
		assert.Contains(t, err.Error(), "invalid syntax")
	}
}

func TestMigrateWithDirectoryNameContainsWhitespaces(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "sqlite3.db")
	t.Logf("DB path : %s\n", dbPath)
	p := &Sqlite{}
	addr := fmt.Sprintf("sqlite3://file:%s", dbPath)
	d, err := p.Open(addr)
	if err != nil {
		t.Fatal(err)
	}
	dt.Test(t, d, []byte("CREATE TABLE t (Qty int, Name string);"))
}
