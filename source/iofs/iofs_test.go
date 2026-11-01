//go:build go1.16

package iofs_test

import (
	"strings"
	"testing"
	"testing/fstest"

	"github.com/golang-migrate/migrate/v4/source/iofs"
	st "github.com/golang-migrate/migrate/v4/source/testing"
)

func Test(t *testing.T) {
	// reuse the embed.FS set in example_test.go
	d, err := iofs.New(fs, "testdata/migrations")
	if err != nil {
		t.Fatal(err)
	}

	st.Test(t, d)
}

// Regression test for https://github.com/golang-migrate/migrate/issues/1012:
// a file that matches the migration naming scheme but whose version number
// cannot be parsed (e.g. it overflows uint64) must not be silently ignored.
func TestNewWithUnparseableMigrationVersion(t *testing.T) {
	fsys := fstest.MapFS{
		"1_foobar.up.sql":   &fstest.MapFile{Data: []byte("1 up")},
		"1_foobar.down.sql": &fstest.MapFile{Data: []byte("1 down")},
		// matches the migration regex, but the version does not fit in uint64
		"99999999999999999999999_too_large.up.sql": &fstest.MapFile{Data: []byte("up")},
	}

	_, err := iofs.New(fsys, ".")
	if err == nil {
		t.Fatal("expected an error for migration file with unparseable version, got nil")
	}
	if !strings.Contains(err.Error(), "99999999999999999999999_too_large.up.sql") {
		t.Errorf("expected error to name the offending file, got: %v", err)
	}
}

func TestNewIgnoresNonMigrationFiles(t *testing.T) {
	fsys := fstest.MapFS{
		"1_foobar.up.sql":   &fstest.MapFile{Data: []byte("1 up")},
		"1_foobar.down.sql": &fstest.MapFile{Data: []byte("1 down")},
		"README.md":         &fstest.MapFile{Data: []byte("readme")},
	}

	d, err := iofs.New(fsys, ".")
	if err != nil {
		t.Fatalf("expected non-migration files to be ignored, got: %v", err)
	}
	v, err := d.First()
	if err != nil {
		t.Fatalf("expected first version to exist, got: %v", err)
	}
	if v != 1 {
		t.Errorf("expected first version to be 1, got %v", v)
	}
}
