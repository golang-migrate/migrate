package httpfs_test

import (
	"errors"
	"net/http"
	"os"
	"testing"

	"github.com/golang-migrate/migrate/v4/source"
	"github.com/golang-migrate/migrate/v4/source/httpfs"
	st "github.com/golang-migrate/migrate/v4/source/testing"
)

type driver struct{ httpfs.Migrator }

func (d *driver) Open(url string) (source.Driver, error) { return nil, errors.New("X") }

func TestMigratorInit(t *testing.T) {
	tests := []struct {
		name string
		fs   http.FileSystem
		path string
		ok   bool
	}{
		{
			name: "valid dir and empty path",
			fs:   http.Dir("testdata/sql"),
			ok:   true,
		},
		{
			name: "valid dir and non-empty path",
			fs:   http.Dir("testdata"),
			path: "sql",
			ok:   true,
		},
		{
			name: "invalid dir",
			fs:   http.Dir("does-not-exist"),
		},
		{
			name: "file instead of dir",
			fs:   http.Dir("testdata/sql/1_foobar.up.sql"),
		},
		{
			name: "dir with duplicates",
			fs:   http.Dir("testdata/duplicates"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var d driver
			err := d.Init(test.fs, test.path)
			if test.ok {
				if err != nil {
					t.Errorf("Init() returned error %s", err)
				}
				st.Test(t, &d)
				if err = d.Close(); err != nil {
					t.Errorf("Init().Close() returned error %s", err)
				}
			} else {
				if err == nil {
					t.Errorf("Init() expected error but did not get one")
				}
			}
		})
	}

}

func TestFirstWithNoMigrations(t *testing.T) {
	var d driver
	fs := http.Dir("testdata/no-migrations")

	if err := d.Init(fs, ""); err != nil {
		t.Errorf("No error on Init() expected, got: %v", err)
	}

	if _, err := d.First(); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("Expected os.ErrNotExist error on First(), got: %v", err)
	}
}

func TestOpen(t *testing.T) {
	d := httpfs.New(http.Dir("testdata/sql"), "")
	d, err := d.Open("")
	if d != nil {
		t.Error("Expected Open to return nil driver")
	}
	if err == nil {
		t.Error("Expected Open to return error")
	}
}
