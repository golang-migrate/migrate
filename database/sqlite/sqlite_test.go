package sqlite

import (
	"database/sql"
	"fmt"
	nurl "net/url"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/golang-migrate/migrate/v4"
	dt "github.com/golang-migrate/migrate/v4/database/testing"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	_ "modernc.org/sqlite"
)

func Test(t *testing.T) {
	dir := t.TempDir()
	t.Logf("DB path : %s\n", filepath.Join(dir, "sqlite.db"))
	p := &Sqlite{}
	addr := fmt.Sprintf("sqlite://%s", filepath.Join(dir, "sqlite.db"))
	d, err := p.Open(addr)
	if err != nil {
		t.Fatal(err)
	}
	dt.Test(t, d, []byte("CREATE TABLE t (Qty int, Name string);"))
}

func TestOpenWithPercentEncodedPath(t *testing.T) {
	// Regression for https://github.com/golang-migrate/migrate/issues/1256:
	// a percent-encoded path (a space encoded as %20) must resolve to the real
	// file name instead of a literal "%20".
	dir := filepath.Join(t.TempDir(), "Magic Data")
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	dbPath := filepath.Join(dir, "sqlite.db")

	addr := (&nurl.URL{Scheme: "sqlite", Path: dbPath}).String()
	p := &Sqlite{}
	d, err := p.Open(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := d.Close(); err != nil {
			t.Error(err)
		}
	}()

	if _, err := os.Stat(dbPath); err != nil {
		t.Fatalf("expected database file at %q: %v", dbPath, err)
	}
	dt.Test(t, d, []byte("CREATE TABLE t (Qty int, Name string);"))
}

func TestMigrate(t *testing.T) {
	dir := t.TempDir()
	t.Logf("DB path : %s\n", filepath.Join(dir, "sqlite.db"))

	db, err := sql.Open("sqlite", filepath.Join(dir, "sqlite.db"))
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

	t.Logf("DB path : %s\n", filepath.Join(dir, "sqlite.db"))

	db, err := sql.Open("sqlite", filepath.Join(dir, "sqlite.db"))
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
	t.Logf("DB path : %s\n", filepath.Join(dir, "sqlite.db"))
	p := &Sqlite{}
	addr := fmt.Sprintf("sqlite://%s?x-no-tx-wrap=true", filepath.Join(dir, "sqlite.db"))
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
	t.Logf("DB path : %s\n", filepath.Join(dir, "sqlite.db"))
	p := &Sqlite{}
	addr := fmt.Sprintf("sqlite://%s?x-no-tx-wrap=yeppers", filepath.Join(dir, "sqlite.db"))
	_, err := p.Open(addr)
	if assert.Error(t, err) {
		assert.Contains(t, err.Error(), "x-no-tx-wrap")
		assert.Contains(t, err.Error(), "invalid syntax")
	}
}

func TestMigrateWithDirectoryNameContainsWhitespaces(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "sqlite.db")
	t.Logf("DB path : %s\n", dbPath)
	p := &Sqlite{}
	addr := fmt.Sprintf("sqlite://file:%s", dbPath)
	d, err := p.Open(addr)
	if err != nil {
		t.Fatal(err)
	}
	dt.Test(t, d, []byte("CREATE TABLE t (Qty int, Name string);"))
}
