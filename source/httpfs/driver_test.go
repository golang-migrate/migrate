package httpfs

import (
	"net/http"
	"testing"

	st "github.com/golang-migrate/migrate/v4/source/testing"
)

func TestWithInstanceAndNew(t *testing.T) {
	tests := []struct {
		msg  string
		fs   http.FileSystem
		path string
		ok   bool
	}{
		{
			msg: "valid dir, empty path",
			fs:  http.Dir("testdata/sql"),
			ok:  true,
		},
		{
			msg:  "valid dir, non-empty path",
			fs:   http.Dir("testdata"),
			path: "sql",
			ok:   true,
		},
		{
			msg: "invalid dir",
			fs:  http.Dir("does-not-exist"),
		},
		{
			msg: "file instead of dir",
			fs:  http.Dir("testdata/sql/1_foobar.up.sql"),
		},
		{
			msg: "dir with duplicates",
			fs:  http.Dir("testdata/duplicates"),
		},
	}

	for _, test := range tests {
		d, err := WithInstance(test.fs, test.path)
		if test.ok {
			if err != nil {
				t.Errorf("%s, WithInstance() returned error %s", test.msg, err)
			}
			st.Test(t, d)
			if err = d.Close(); err != nil {
				t.Errorf("%s, WithInstance().Close() returned error %s", test.msg, err)
			}
		}
		if !test.ok {
			if err == nil {
				t.Errorf("%s, WithInstance() expected error but did not get one", test.msg)
			}
		}
	}

	for _, test := range tests {
		d := New(test.fs, test.path)
		if test.ok {
			st.Test(t, d)
		}
		if !test.ok {
			if _, err := d.Open(""); err == nil {
				t.Errorf("%s, New().Open() expected error but did not get one", test.msg)
			}
			if err := d.Close(); err == nil {
				t.Errorf("%s, New().Close() expected error but did not get one", test.msg)
			}
			if _, err := d.First(); err == nil {
				t.Errorf("%s, New().First() expected error but did not get one", test.msg)
			}
			if _, err := d.Prev(0); err == nil {
				t.Errorf("%s, New().Prev() expected error but did not get one", test.msg)
			}
			if _, err := d.Next(0); err == nil {
				t.Errorf("%s, New().Next() expected error but did not get one", test.msg)
			}
			if _, _, err := d.ReadUp(0); err == nil {
				t.Errorf("%s, New().ReadUp() expected error but did not get one", test.msg)
			}
			if _, _, err := d.ReadDown(0); err == nil {
				t.Errorf("%s, New().ReadDown() expected error but did not get one", test.msg)
			}
		}
	}

}

func TestOpen(t *testing.T) {
	b := &driver{}
	d, err := b.Open("")
	if d != nil {
		t.Error("Expected Open to return nil driver")
	}
	if err == nil {
		t.Error("Expected Open to return error")
	}
}
