// Package testing has the database tests.
// All database drivers must pass the Test function.
// This lives in it's own package so it stays a test dependency.
package testing

import (
	"bytes"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4/database"
)

// Test runs tests against database implementations.
func Test(t *testing.T, d database.Driver, migration []byte) {
	if migration == nil {
		t.Fatal("test must provide migration reader")
	}

	TestNilVersion(t, d) // test first
	TestLockAndUnlock(t, d)
	TestRun(t, d, bytes.NewReader(migration))
	TestSetVersion(t, d) // also tests Version()
	// Drop breaks the driver, so test it last.
	TestDrop(t, d)
}

func TestNilVersion(t *testing.T, d database.Driver) {
	v, _, err := d.Version()
	if err != nil {
		t.Fatal(err)
	}
	if v != database.NilVersion {
		t.Fatalf("Version: expected version to be NilVersion (-1), got %v", v)
	}
}

func TestLockAndUnlock(t *testing.T, d database.Driver) {
	// add a timeout, in case there is a deadlock
	done := make(chan bool, 1)
	go func() {
		timeout := time.After(15 * time.Second)
		for {
			select {
			case <-done:
				return
			case <-timeout:
				t.Fatal(fmt.Sprintf("Timeout after 15 seconds. Looks like a deadlock in Lock/UnLock.\n%#v", d))
			}
		}
	}()
	defer func() {
		done <- true
	}()

	// run the locking test ...

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
	if migration == nil {
		t.Fatal("migration can't be nil")
	}

	if err := d.Run(migration); err != nil {
		t.Fatal(err)
	}
}

func TestDrop(t *testing.T, d database.Driver) {
	if err := d.Drop(); err != nil {
		t.Fatal(err)
	}
}

func TestSetVersion(t *testing.T, d database.Driver) {
	if err := d.SetVersion(1, true); err != nil {
		t.Fatal(err)
	}

	// call again
	if err := d.SetVersion(1, true); err != nil {
		t.Fatal(err)
	}

	v, dirty, err := d.Version()
	if err != nil {
		t.Fatal(err)
	}
	if !dirty {
		t.Fatal("expected dirty")
	}
	if v != 1 {
		t.Fatal("expected version to be 1")
	}

	if err := d.SetVersion(2, false); err != nil {
		t.Fatal(err)
	}

	// call again
	if err := d.SetVersion(2, false); err != nil {
		t.Fatal(err)
	}

	v, dirty, err = d.Version()
	if err != nil {
		t.Fatal(err)
	}
	if dirty {
		t.Fatal("expected not dirty")
	}
	if v != 2 {
		t.Fatal("expected version to be 2")
	}
}
