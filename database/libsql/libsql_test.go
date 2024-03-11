package libsql

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/golang-migrate/migrate/v4"
	dt "github.com/golang-migrate/migrate/v4/database/testing"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	_ "modernc.org/sqlite"

	"github.com/stretchr/testify/assert"
)

func Test(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "libsql.db")
	t.Logf("DB path : %s\n", dbPath)
	p := &LibSQL{}
	addr := fmt.Sprintf("file:%s", dbPath)
	d, err := p.Open(addr)
	if err != nil {
		t.Fatal(err)
	}
	dt.Test(t, d, []byte("CREATE TABLE t (Qty int, Name string);"))
	dt.TestDrop(t, d)
}

func TestMigrate(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join("file:", dir, "libsql.db")
	t.Logf("DB path : %s\n", dbPath)

	db, err := sql.Open("libsql", dbPath)
	if err != nil {
		t.Errorf("Error opening database: %v", err)
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
	dbPath := filepath.Join("file://", dir, "libsql.db")
	t.Logf("DB path : %s\n", dbPath)

	db, err := sql.Open("libsql", dbPath)
	if err != nil {
		t.Errorf("Error opening database: %v", err)
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
	dbPath := filepath.Join(dir, "libsql.db")
	t.Logf("DB path : %s\n", dbPath)
	p := &LibSQL{}
	addr := fmt.Sprintf("file://%s?x-no-tx-wrap=true", dbPath)
	d, err := p.Open(addr)
	if err != nil {
		t.Fatal(err)
	}
	// An explicit BEGIN statement would ordinarily fail without x-no-tx-wrap.
	// (Transactions in sqlite may not be nested.)
	dt.Test(t, d, []byte("BEGIN; CREATE TABLE t (Qty int, Name string); COMMIT;"))
	dt.TestDrop(t, d)
}

func TestNoTxWrapInvalidValue(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "libsql.db")
	t.Logf("DB path : %s\n", dbPath)
	p := &LibSQL{}
	addr := fmt.Sprintf("libsql://%s?x-no-tx-wrap=yeppers", dbPath)
	_, err := p.Open(addr)
	if assert.Error(t, err) {
		assert.Contains(t, err.Error(), "x-no-tx-wrap")
		assert.Contains(t, err.Error(), "invalid syntax")
	}
}
