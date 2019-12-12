package httpfs_test

import (
	"net/http"
	"testing"

	"github.com/golang-migrate/migrate/v4/source/httpfs"
	st "github.com/golang-migrate/migrate/v4/source/testing"
)

func TestWithInstanceAndNew(t *testing.T) {
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
		t.Run("Init "+test.name, func(t *testing.T) {
			var d httpfs.Driver
			err := d.Init(test.fs, test.path)
			if test.ok {
				if err != nil {
					t.Errorf("WithInstance() returned error %s", err)
				}
				st.Test(t, &d)
				if err = d.Close(); err != nil {
					t.Errorf("WithInstance().Close() returned error %s", err)
				}
			}
			if !test.ok {
				if err == nil {
					t.Errorf("WithInstance() expected error but did not get one")
				}
			}
		})
	}

	for _, test := range tests {
		t.Run("New "+test.name, func(t *testing.T) {
			d := httpfs.New(test.fs, test.path)
			if test.ok {
				st.Test(t, d)
			}
			if !test.ok {
				if _, err := d.Open(""); err == nil {
					t.Errorf("Open() expected error but did not get one")
				}
				if err := d.Close(); err == nil {
					t.Errorf("Close() expected error but did not get one")
				}
				if _, err := d.First(); err == nil {
					t.Errorf("First() expected error but did not get one")
				}
				if _, err := d.Prev(0); err == nil {
					t.Errorf("Prev() expected error but did not get one")
				}
				if _, err := d.Next(0); err == nil {
					t.Errorf("Next() expected error but did not get one")
				}
				if _, _, err := d.ReadUp(0); err == nil {
					t.Errorf("ReadUp() expected error but did not get one")
				}
				if _, _, err := d.ReadDown(0); err == nil {
					t.Errorf("ReadDown() expected error but did not get one")
				}
			}
		})
	}

}

func TestOpen(t *testing.T) {
	b := &httpfs.Driver{}
	d, err := b.Open("")
	if d != nil {
		t.Error("Expected Open to return nil driver")
	}
	if err == nil {
		t.Error("Expected Open to return error")
	}
}
