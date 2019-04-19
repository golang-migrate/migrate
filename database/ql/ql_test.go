package ql

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	_ "github.com/cznic/ql/driver"
	"github.com/golang-migrate/migrate/v4"
	dt "github.com/golang-migrate/migrate/v4/database/testing"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

func Test(t *testing.T) {
	dir, err := ioutil.TempDir("", "ql-driver-test")
	if err != nil {
		return
	}
	defer func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Fatal(err)
		}
	}()
	t.Logf("DB path : %s\n", filepath.Join(dir, "ql.db"))
	p := &Ql{}
	addr := fmt.Sprintf("ql://%s", filepath.Join(dir, "ql.db"))
	d, err := p.Open(addr)
	if err != nil {
		t.Fatal(err)
	}

	db, err := sql.Open("ql", filepath.Join(dir, "ql.db"))
	if err != nil {
		return
	}
	defer func() {
		if err := db.Close(); err != nil {
			return
		}
	}()
	dt.Test(t, d, []byte("CREATE TABLE t (Qty int, Name string);"))
}

func TestMigrate(t *testing.T) {
	dir, err := ioutil.TempDir("", "ql-driver-test")
	if err != nil {
		return
	}
	defer func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Error(err)
		}
	}()
	t.Logf("DB path : %s\n", filepath.Join(dir, "ql.db"))

	db, err := sql.Open("ql", filepath.Join(dir, "ql.db"))
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
	dt.TestMigrate(t, m, []byte("CREATE TABLE t (Qty int, Name string);"))
}
