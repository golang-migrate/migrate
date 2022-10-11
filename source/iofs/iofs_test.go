//go:build go1.16
// +build go1.16

package iofs_test

import (
	"embed"
	"testing"

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

//go:embed testdata/migrations-tree/*
var fsSub embed.FS

func TestRecursive(t *testing.T) {
	d, err := iofs.New(fsSub, "testdata/migrations-tree/*")
	if err != nil {
		t.Fatal(err)
	}

	st.Test(t, d)
}

//go:embed testdata/migrations-duplicate-tree/*/*.sql
var fsSubDup embed.FS

func TestRecursiveDuplicate(t *testing.T) {
	_, err := iofs.New(fsSubDup, "testdata/migrations-duplicate-tree/*")
	if err == nil {
		t.Fatal(err)
	}
}
