package httpfs_test

import (
	"net/http"
	"testing"

	"github.com/golang-migrate/migrate/v4/source/httpfs"
	st "github.com/golang-migrate/migrate/v4/source/testing"
)

func TestNewOK(t *testing.T) {
	d, err := httpfs.New(http.Dir("testdata"), "sql")
	if err != nil {
		t.Errorf("New() expected not error, got: %s", err)
	}
	st.Test(t, d)
}

func TestNewErrors(t *testing.T) {
	d, err := httpfs.New(http.Dir("does-not-exist"), "")
	if err == nil {
		t.Errorf("New() expected to return error")
	}
	if d != nil {
		t.Errorf("New() expected to return nil driver")
	}
}

func TestOpen(t *testing.T) {
	d, err := httpfs.New(http.Dir("testdata/sql"), "")
	if err != nil {
		t.Error("New() expected no error")
		return
	}
	d, err = d.Open("")
	if d != nil {
		t.Error("Open() expected to return nil driver")
	}
	if err == nil {
		t.Error("Open() expected to return error")
	}
}
