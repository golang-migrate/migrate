// +build go1.16

package iofs_test

import (
	"embed"
	"testing"

	"github.com/golang-migrate/migrate/v4/source/iofs"
	st "github.com/golang-migrate/migrate/v4/source/testing"
)

//go:embed testdata
var fs embed.FS

func Test(t *testing.T) {
	d, err := iofs.WithInstance(fs, "testdata")
	if err != nil {
		t.Fatal(err)
	}

	st.Test(t, d)
}

func TestOpen(t *testing.T) {
	i := new(iofs.IoFS)
	_, err := i.Open("")
	if err == nil {
		t.Fatal("iofs driver does not support open by url")
	}
}
