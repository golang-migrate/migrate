package turso

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/golang-migrate/migrate/v4"
	dt "github.com/golang-migrate/migrate/v4/database/testing"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	_ "turso.tech/database/tursogo"
)

const testEncryptionKey = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

// Test runs the standard database/testing test suite.
func Test(t *testing.T) {
	dir := t.TempDir()
	t.Logf("DB path : %s\n", filepath.Join(dir, "turso.db"))
	p := &Turso{}
	addr := fmt.Sprintf("turso://%s", filepath.Join(dir, "turso.db"))
	d, err := p.Open(addr)
	if err != nil {
		t.Fatal(err)
	}
	dt.Test(t, d, []byte("CREATE TABLE t (Qty int, Name text);"))
}

// TestMigrate runs end-to-end migration with the standard test suite.
func TestMigrate(t *testing.T) {
	dir := t.TempDir()
	t.Logf("DB path : %s\n", filepath.Join(dir, "turso.db"))

	db, err := sql.Open("turso", filepath.Join(dir, "turso.db"))
	if err != nil {
		t.Fatal(err)
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
		"turso", driver)
	if err != nil {
		t.Fatal(err)
	}
	dt.TestMigrate(t, m)
}

// TestMigrationTable verifies custom migration table name support.
func TestMigrationTable(t *testing.T) {
	dir := t.TempDir()
	t.Logf("DB path : %s\n", filepath.Join(dir, "turso.db"))

	db, err := sql.Open("turso", filepath.Join(dir, "turso.db"))
	if err != nil {
		t.Fatal(err)
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
		"turso", driver)
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

// TestNoTxWrap verifies that x-no-tx-wrap=true allows explicit transactions.
func TestNoTxWrap(t *testing.T) {
	dir := t.TempDir()
	t.Logf("DB path : %s\n", filepath.Join(dir, "turso.db"))
	p := &Turso{}
	addr := fmt.Sprintf("turso://%s?x-no-tx-wrap=true", filepath.Join(dir, "turso.db"))
	d, err := p.Open(addr)
	if err != nil {
		t.Fatal(err)
	}
	// An explicit BEGIN statement would ordinarily fail without x-no-tx-wrap.
	// (Transactions in sqlite may not be nested.)
	dt.Test(t, d, []byte("BEGIN; CREATE TABLE t (Qty int, Name text); COMMIT;"))
}

// TestNoTxWrapInvalidValue verifies bad x-no-tx-wrap values are rejected.
func TestNoTxWrapInvalidValue(t *testing.T) {
	dir := t.TempDir()
	t.Logf("DB path : %s\n", filepath.Join(dir, "turso.db"))
	p := &Turso{}
	addr := fmt.Sprintf("turso://%s?x-no-tx-wrap=yeppers", filepath.Join(dir, "turso.db"))
	_, err := p.Open(addr)
	if assert.Error(t, err) {
		assert.Contains(t, err.Error(), "x-no-tx-wrap")
		assert.Contains(t, err.Error(), "invalid syntax")
	}
}

// TestCustomTypes verifies that Turso custom types (uuid, varchar, boolean,
// timestamp, json) and STRICT tables work end-to-end.
func TestCustomTypes(t *testing.T) {
	turso := &Turso{}
	driver, err := turso.Open("turso://:memory:?x-experimental=custom_types")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = driver.Close() }()

	m, err := migrate.NewWithDatabaseInstance(
		"file://./examples/migrations-custom-types", "turso", driver)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Up(); err != nil {
		t.Fatal(err)
	}

	// Verify we can also migrate down cleanly.
	if err := m.Down(); err != nil {
		t.Fatal(err)
	}
}

// TestCustomTypesWithoutFlag tests whether custom types work without the
// experimental=custom_types flag. Documents the finding for the README.
func TestCustomTypesWithoutFlag(t *testing.T) {
	turso := &Turso{}
	driver, err := turso.Open("turso://:memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = driver.Close() }()

	m, err := migrate.NewWithDatabaseInstance(
		"file://./examples/migrations-custom-types", "turso", driver)
	if err != nil {
		t.Fatal(err)
	}
	err = m.Up()
	if err != nil {
		t.Logf("custom types WITHOUT experimental=custom_types flag: FAILED (%v)", err)
		t.Logf("This confirms the flag is still required in turso-go v0.5.x")
	} else {
		t.Logf("custom types WITHOUT experimental=custom_types flag: PASSED")
		t.Logf("The flag is no longer required — update README accordingly")
	}
}

// TestEncryption verifies AEGIS-256 encryption at rest:
// - Opens an encrypted on-disk database, runs a migration, closes.
// - Re-opens with the correct key — schema_migrations table is readable.
// - Re-opens with the wrong key — fails as expected.
func TestEncryption(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "encrypted.db")

	urlWithKey := "turso://" + dbPath +
		"?x-experimental=encryption,custom_types" +
		"&x-encryption-cipher=aegis256" +
		"&x-encryption-hexkey=" + testEncryptionKey

	turso := &Turso{}
	driver, err := turso.Open(urlWithKey)
	if err != nil {
		t.Fatal(err)
	}
	m, err := migrate.NewWithDatabaseInstance(
		"file://./examples/migrations", "turso", driver)
	if err != nil {
		_ = driver.Close()
		t.Fatal(err)
	}
	if err := m.Up(); err != nil {
		_ = driver.Close()
		t.Fatal(err)
	}
	_ = driver.Close()

	// Re-open with correct key — should succeed.
	driver2, err := (&Turso{}).Open(urlWithKey)
	if err != nil {
		t.Fatalf("re-open with correct key: %v", err)
	}
	v, _, err := driver2.Version()
	if err != nil || v < 0 {
		t.Fatalf("expected version >= 0, got version=%d err=%v", v, err)
	}
	t.Logf("re-open with correct key: version=%d (OK)", v)
	_ = driver2.Close()

	// Re-open with wrong key — should fail.
	wrongKey := strings.Repeat("f", 64)
	urlWrong := "turso://" + dbPath +
		"?x-experimental=encryption,custom_types" +
		"&x-encryption-cipher=aegis256" +
		"&x-encryption-hexkey=" + wrongKey
	driver3, err := (&Turso{}).Open(urlWrong)
	if err == nil {
		// Some drivers fail at open, some at first read — try a read.
		_, _, err = driver3.Version()
		_ = driver3.Close()
	}
	if err == nil {
		t.Fatal("expected open or read with wrong key to fail")
	}
	t.Logf("wrong key correctly rejected: %v", err)

	_ = os.Remove(dbPath)
}

// TestOpenURL verifies file-based on-disk databases work (catches things
// that in-memory tests miss).
func TestOpenURL(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	turso := &Turso{}
	driver, err := turso.Open("turso://" + dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = driver.Close() }()

	if err := driver.Run(strings.NewReader("CREATE TABLE foo (id int);")); err != nil {
		t.Fatal(err)
	}
}
