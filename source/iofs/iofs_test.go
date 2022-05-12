//go:build go1.16
// +build go1.16

package iofs_test

import (
	"embed"
	"testing"

	"github.com/getoutreach/migrate/v4/source/iofs"
	st "github.com/getoutreach/migrate/v4/source/testing"
)

//go:embed testdata/migrations/*.sql
var fs embed.FS

func Test(t *testing.T) {
	// reuse the embed.FS set in example_test.go
	d, err := iofs.New(fs, "testdata/migrations")
	if err != nil {
		t.Fatal(err)
	}

	st.Test(t, d)
}
