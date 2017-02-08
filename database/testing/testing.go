package testing

import (
	"bytes"
	"io"
	"testing"

	"github.com/mattes/migrate/database"
)

func Test(t *testing.T, d database.Driver, migration []byte) {
	if migration == nil {
		panic("test must provide migration reader")
	}

	TestNilVersion(t, d) // test first
	TestLockAndUnlock(t, d)
	TestRun(t, d, bytes.NewReader(migration)) // also tests Drop()
	TestRunWithNilVersion(t, d, bytes.NewReader(migration))
	TestRunWithNilMigration(t, d)
}

func TestNilVersion(t *testing.T, d database.Driver) {
	v, err := d.Version()
	if err != nil {
		t.Fatal(err)
	}
	if v != database.NilVersion {
		t.Fatalf("Version: expected version to be NilVersion (-1), got %v", v)
	}
}

func TestLockAndUnlock(t *testing.T, d database.Driver) {
	// TODO: add timeouts, in case something goes wrong
	if err := d.Lock(); err != nil {
		t.Fatal(err)
	}

	// try to acquire lock again
	if err := d.Lock(); err == nil {
		t.Fatal("Lock: expected err not to be nil")
	}

	// unlock
	if err := d.Unlock(); err != nil {
		t.Fatal(err)
	}

	// try to lock
	if err := d.Lock(); err != nil {
		t.Fatal(err)
	}
	if err := d.Unlock(); err != nil {
		t.Fatal(err)
	}
}

func TestRun(t *testing.T, d database.Driver, migration io.Reader) {
	// Run migration
	err := d.Run(1485475009, migration)
	if err != nil {
		t.Fatal(err)
	}

	// Check version
	version, err := d.Version()
	if err != nil {
		t.Fatal(err)
	}
	if version != 1485475009 {
		t.Fatalf("Version: expected 1485475009, got %v", version)
	}

	// Drop everything
	if err := d.Drop(); err != nil {
		t.Fatal(err)
	}

	// Check version again
	if v, err := d.Version(); err != nil {
		t.Fatal(err)
	} else if v != database.NilVersion {
		t.Fatalf("Version: expected version to be NilVersion (-1), got %v", v)
	}
}

func TestRunWithNilVersion(t *testing.T, d database.Driver, migration io.Reader) {
	// Run migration
	err := d.Run(database.NilVersion, migration)
	if err != nil {
		t.Fatal(err)
	}

	// Check version
	version, err := d.Version()
	if err != nil {
		t.Fatal(err)
	}
	if version != database.NilVersion {
		t.Fatalf("Version: expected database.NilVersion (-1), got %v", version)
	}
}

func TestRunWithNilMigration(t *testing.T, d database.Driver) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatal("got panic, make sure to handle nil migration io.Reader")
		}
	}()

	// Run with nil migration
	err := d.Run(1486242612, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Check version
	version, err := d.Version()
	if err != nil {
		t.Fatal(err)
	}
	if version != 1486242612 {
		t.Fatalf("TestRunWithNilMigration: expected version 1486242612, got %v", version)
	}
}
