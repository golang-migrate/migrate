// +build go1.16

package iofs_test

import (
	"embed"
	"testing"

	"github.com/golang-migrate/migrate/v4/source/iofs"
	st "github.com/golang-migrate/migrate/v4/source/testing"
)

func Test(t *testing.T) {
	//go:embed testdata/migrations/*.sql
	var fs embed.FS
	d, err := iofs.New(fs, "testdata/migrations")
	if err != nil {
		t.Fatal(err)
	}

	st.Test(t, d)
}
