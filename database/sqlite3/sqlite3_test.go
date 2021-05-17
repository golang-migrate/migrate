package sqlite3

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	nurl "net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/golang-migrate/migrate/v4"
	dt "github.com/golang-migrate/migrate/v4/database/testing"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	_ "github.com/mattn/go-sqlite3"
)

func Test(t *testing.T) {
	dir, err := ioutil.TempDir("", "sqlite3-driver-test")
	if err != nil {
		return
	}
	defer func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Error(err)
		}
	}()
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
	dir, err := ioutil.TempDir("", "sqlite3-driver-test")
	if err != nil {
		return
	}
	defer func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Error(err)
		}
	}()
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
	dir, err := ioutil.TempDir("", "sqlite3-driver-test-migration-table")
	if err != nil {
		return
	}
	defer func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Error(err)
		}
	}()

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
	dir, err := ioutil.TempDir("", "sqlite3-driver-test")
	if err != nil {
		return
	}
	defer func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Error(err)
		}
	}()
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
	dir, err := ioutil.TempDir("", "sqlite3-driver-test")
	if err != nil {
		return
	}
	defer func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Error(err)
		}
	}()
	t.Logf("DB path : %s\n", filepath.Join(dir, "sqlite3.db"))
	p := &Sqlite{}
	addr := fmt.Sprintf("sqlite3://%s?x-no-tx-wrap=yeppers", filepath.Join(dir, "sqlite3.db"))
	_, err = p.Open(addr)
	if assert.Error(t, err) {
		assert.Contains(t, err.Error(), "x-no-tx-wrap")
		assert.Contains(t, err.Error(), "invalid syntax")
	}
}

func TestMigrateWithDirectoryNameContainsWhitespaces(t *testing.T) {
	dir, err := ioutil.TempDir("", "directory name contains whitespaces")
	if err != nil {
		return
	}
	defer func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Error(err)
		}
	}()
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

func TestDbPathOutput(t *testing.T) {

	var pathTests = []struct {
		name string
		in   string
		out  string
	}{
		// Test inputs will be passed through `net/url` Parse()
		// Outputs will be the URLs parsed by dbPathFromURL()
		//
		// When designing a test that will fail to be parsed (ie, expect net/url.Parse() to throw an error),
		// we will match the `out` string as either the prefix or suffix of the error string, otherwise the test will fail.

		// simple path tests - no schema
		{"simple path (no schema)",
			"/Path/To/A/DB/file.db", "/Path/To/A/DB/file.db"},
		{"simple path (no schema), with whitespaces",
			"/Path To/A DB/file name.db", "/Path To/A DB/file name.db"},

		// simple path tests - relative
		{"simple path (relative)",
			"sqlite3://Path/To/A/DB/file.db", "Path/To/A/DB/file.db"},
		{"simple path (relative), with whitespaces",
			"sqlite3://Path/To/A DB/file name.db", "Path/To/A DB/file name.db"},
		{"simple path (relative), with invalid host",
			"sqlite3://Path To/A DB/file name.db", "invalid character \" \" in host name"},

		// simple path tests - absolute
		{"simple valid path, no whitespaces",
			"sqlite3:///Path/To/A/DB/file.db", "/Path/To/A/DB/file.db"},
		{"simple path, with whitespaces",
			"sqlite3:///Path To/A DB/file name.db", "/Path To/A DB/file name.db"},

		// path w/query param tests
		{"path with whitespaces and query params",
			"sqlite3:///Path To/A DB/file name.db?aQuery=something&bQuery=else&c=d", "/Path To/A DB/file name.db?aQuery=something&bQuery=else&c=d"},
		{"path with whitespaces and query params that require escaping",
			"sqlite3:///Path To/A DB/file name.db?aQuery=\"something\"&bQuery=else&c=d", "/Path To/A DB/file name.db?aQuery=%22something%22&bQuery=else&c=d"},
		{"path with whitespaces and query params (including custom query param that should be filtered out)",
			"sqlite3:///Path To/A DB/file name.db?aQuery=something&bQuery=else&c=d&x-custom-query-param=scrubbed", "/Path To/A DB/file name.db?aQuery=something&bQuery=else&c=d"},

		// path with % escaped character tests
		{"path with % escaped characters",
			"sqlite3:///Path%20To/A%20DB/file%20name.db", "/Path To/A DB/file name.db"},
		{"path with % escaped characters & escaped query params",
			"sqlite3:///Path%20To/A%20DB/file%20name.db?aQuery=something%20else&c=d", "/Path To/A DB/file name.db?aQuery=something+else&c=d"},
	}

	for _, tt := range pathTests {
		t.Run(tt.name, func(t *testing.T) {
			inputURL, err := nurl.Parse(tt.in)
			if err != nil {
				if strings.HasPrefix(err.Error(), tt.out) ||
					strings.HasSuffix(err.Error(), tt.out) {
					// The input string cannot be parsed by net/url and
					// and the test case expected that.
					return
				}
				t.Errorf("`in` string failed to parse into a valid URL:  %v", err)
				return
			}

			s := dbPathFromURL(inputURL)
			if s != tt.out {
				t.Errorf("expected: %q, actual: %q", tt.out, s)
			}
		})
	}
}
