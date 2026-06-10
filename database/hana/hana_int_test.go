package hana

import (
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"

	hdbDriver "github.com/SAP/go-hdb/driver"
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
	dt "github.com/golang-migrate/migrate/v4/database/testing"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

const envKey = "HANA_DATABASE_URL"

type closeFunc func()

func TestDriver(t *testing.T) {
	_, d, close := setupTesting(t)
	defer close()

	dt.Test(t, d, []byte("CREATE TABLE TEST_DRIVER ( ID INT NOT NULL, PRIMARY KEY (ID) );"))
}

func TestMigrate(t *testing.T) {
	m, _, close := setupTesting(t)
	defer close()

	dt.TestMigrate(t, m)
}

func setupTesting(t *testing.T) (*migrate.Migrate, database.Driver, closeFunc) {
	t.Helper()

	rawURL := getURL(t)
	schemaName := getSchemaFromURL(t, rawURL)
	ensureTestSchema(t, rawURL, schemaName)

	p := &Hana{}
	d, err := p.Open(rawURL)
	if err != nil {
		t.Fatal(err)
	}

	close := func() {
		if err := d.Close(); err != nil {
			t.Error(err)
		}
	}

	m, err := migrate.NewWithDatabaseInstance("file://./examples/migrations", "hdb", d)
	if err != nil {
		t.Fatal(err)
	}

	return m, d, close
}

func ensureTestSchema(t *testing.T, rawURL string, schemaName string) {
	t.Helper()

	db := openWithoutSchema(t, rawURL)
	defer db.Close()

	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM SYS.SCHEMAS WHERE SCHEMA_NAME = ?`, schemaName).Scan(&count); err != nil {
		t.Fatalf("failed to check schema existence: %v", err)
	}

	if count > 0 {
		return
	}

	query := fmt.Sprintf("CREATE SCHEMA %s", hdbDriver.Identifier(schemaName))
	if _, err := db.Exec(query); err != nil {
		t.Fatalf("failed to create test schema %s: %v", schemaName, err)
	}
}

func openWithoutSchema(t *testing.T, rawURL string) *sql.DB {
	t.Helper()

	purl, err := url.Parse(rawURL)
	if err != nil {
		t.Fatalf("failed to parse URL: %v", err)
	}

	// filter out all x- prefixed parameters
	dsn := migrate.FilterCustomQuery(purl).String()
	connector, err := hdbDriver.NewDSNConnector(dsn)
	if err != nil {
		t.Fatalf("failed to create connector: %v", err)
	}

	db := sql.OpenDB(connector)
	if err := db.Ping(); err != nil {
		t.Fatalf("failed to ping database: %v", err)
	}

	return db
}

func getSchemaFromURL(t *testing.T, rawURL string) string {
	t.Helper()

	purl, err := url.Parse(rawURL)
	if err != nil {
		t.Fatalf("failed to parse URL: %v", err)
	}

	schema := purl.Query().Get("x-migrations-schema")
	if schema == "" {
		t.Fatal("HANA_DATABASE_URL must include x-migrations-schema parameter")
	}

	return schema
}

func TestSchemaMismatch(t *testing.T) {
	rawURL := getURL(t)
	schemaName := getSchemaFromURL(t, rawURL)
	ensureTestSchema(t, rawURL, schemaName)

	// open a DB connection with the correct schema set on the connector
	purl, err := url.Parse(rawURL)
	if err != nil {
		t.Fatal(err)
	}

	dsn := migrate.FilterCustomQuery(purl).String()
	connector, err := hdbDriver.NewDSNConnector(dsn)
	if err != nil {
		t.Fatal(err)
	}

	connector.SetDefaultSchema(schemaName)
	db := sql.OpenDB(connector)
	defer db.Close()

	// pass a different schema name in config
	_, err = WithInstance(db, &Config{
		SchemaName: "NONEXISTENT_SCHEMA_THAT_DOES_NOT_MATCH",
	})
	if !errors.Is(err, ErrSchemaMismatch) {
		t.Fatalf("expected ErrSchemaMismatch, got: %v", err)
	}
}

func TestMultiStatement(t *testing.T) {
	rawURL := getURL(t)
	schemaName := getSchemaFromURL(t, rawURL)
	ensureTestSchema(t, rawURL, schemaName)

	p := &Hana{}
	d, err := p.Open(rawURL)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	defer d.Drop()

	m, err := migrate.NewWithDatabaseInstance("file://./examples/migrations_multi", "hdb", d)
	if err != nil {
		t.Fatal(err)
	}

	err = m.Up()
	if err != nil {
		t.Fatalf("multi-statement up migration failed: %v", err)
	}

	hd := d.(*Hana)
	assertTableExists(t, hd.db, schemaName, "MULTI_A")
	assertTableExists(t, hd.db, schemaName, "MULTI_B")
	assertTableExists(t, hd.db, schemaName, "MULTI_C")
}

func TestMultiStatementCustomDelimiter(t *testing.T) {
	rawURL := getURL(t)
	schemaName := getSchemaFromURL(t, rawURL)
	ensureTestSchema(t, rawURL, schemaName)

	// append custom delimiter to URL
	separator := "&"
	if !strings.Contains(rawURL, "?") {
		separator = "?"
	}
	customURL := rawURL + separator + "x-multi-statement-delimiter=--SPLIT--"

	p := &Hana{}
	d, err := p.Open(customURL)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	defer d.Drop()

	m, err := migrate.NewWithDatabaseInstance("file://./examples/migrations_split", "hdb", d)
	if err != nil {
		t.Fatal(err)
	}

	err = m.Up()
	if err != nil {
		t.Fatalf("multi-statement custom delimiter up migration failed: %v", err)
	}

	hd := d.(*Hana)
	assertTableExists(t, hd.db, schemaName, "SPLIT_A")
	assertTableExists(t, hd.db, schemaName, "SPLIT_B")
	assertTableExists(t, hd.db, schemaName, "SPLIT_C")
}

func TestMultiStatementRollback(t *testing.T) {
	rawURL := getURL(t)
	schemaName := getSchemaFromURL(t, rawURL)
	ensureTestSchema(t, rawURL, schemaName)

	p := &Hana{}
	d, err := p.Open(rawURL)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	defer d.Drop()

	m, err := migrate.NewWithDatabaseInstance("file://./examples/migrations_rollback", "hdb", d)
	if err != nil {
		t.Fatal(err)
	}

	// migration 1: create table (auto-committed)
	err = m.Steps(1)
	if err != nil {
		t.Fatalf("migration 1 (create table) failed: %v", err)
	}

	hd := d.(*Hana)
	assertTableExists(t, hd.db, schemaName, "ROLLBACK_TEST")

	// migration 2: inserts with clashing keys - no row should be added
	err = m.Steps(1)
	if err == nil {
		t.Fatal("expected error from duplicate PK insert, got nil")
	}

	assertTableExists(t, hd.db, schemaName, "ROLLBACK_TEST")

	var count int
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s.%s",
		hdbDriver.Identifier(schemaName), hdbDriver.Identifier("ROLLBACK_TEST"))
	err = hd.db.QueryRow(query).Scan(&count)
	if err != nil {
		t.Fatalf("failed to count rows: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected 0 rows after rollback, got %d", count)
	}

	version, dirty, err := d.Version()
	if err != nil {
		t.Fatalf("failed to get version: %v", err)
	}
	if !dirty {
		t.Fatalf("expected dirty=true, got false (version=%d)", version)
	}
}

func assertTableExists(t *testing.T, db *sql.DB, schemaName, tableName string) {
	t.Helper()
	var count int
	err := db.QueryRow("SELECT COUNT(*) FROM SYS.TABLES WHERE SCHEMA_NAME = ? AND TABLE_NAME = ?",
		schemaName, tableName).Scan(&count)
	if err != nil {
		t.Fatalf("failed to check table %s: %v", tableName, err)
	}
	if count != 1 {
		t.Fatalf("expected table %s to exist", tableName)
	}
}

func getURL(t *testing.T) string {
	t.Helper()
	url := os.Getenv(envKey)
	if url == "" {
		t.Skipf("skipping integration test: %s not set", envKey)
	}

	return url
}
