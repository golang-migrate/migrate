package driver

import "testing"
import _ "github.com/mattes/migrate/driver/bash"
import _ "github.com/mattes/migrate/driver/sqlite3"

func TestNew(t *testing.T) {
	if _, err := New("unknown://url"); err == nil {
		t.Error("no error although driver unknown")
	}
}

func TestNewBash(t *testing.T) {
	driver, err := New("bash://url")
	if err != nil {
		t.Error("error although bash driver known")
	}
	version, err := driver.Version()
	if version != 0 {
		t.Errorf("expected bash driver version to be 0, got %d\n", version)
	}
}

func TestNewSqlite3(t *testing.T) {
	_, err := New("sqlite3://test.db")
	if err != nil {
		t.Error("error although sqlite3 driver known")
	}
}
