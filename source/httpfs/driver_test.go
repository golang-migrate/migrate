package httpfs_test

import (
	"net/http"
	"testing"

	"github.com/golang-migrate/migrate/v4/source/httpfs"
	st "github.com/golang-migrate/migrate/v4/source/testing"
)

func TestNewOK(t *testing.T) {
	d := httpfs.New(http.Dir("testdata"), "sql")
	st.Test(t, d)
}

func TestNewErrors(t *testing.T) {
	d := httpfs.New(http.Dir("does-not-exist"), "")
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
