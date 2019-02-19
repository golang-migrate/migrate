// Package testing has the database tests.
// All database drivers must pass the Test function.
// This lives in it's own package so it stays a test dependency.
package testing

import (
	"fmt"
	"testing"
)

import (
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
	_ "github.com/golang-migrate/migrate/v4/source/stub"
)

// TestMigrate runs integration-tests between the Migrate layer and database implementations.
//
func TestMigrate(t *testing.T, d database.Driver, migration []byte) {
	if migration == nil {
		panic("test must provide migration reader")
	}

	m, err := migrate.NewWithDatabaseInstance("stub://", "", d)
	if err != nil {
		panic(fmt.Sprintf("failed to create migration, due to error: %v", err))
	}
	TestMigrateDrop(t, m)
}

// Regression test for preventing a regression for #164 https://github.com/golang-migrate/migrate/pull/173
// Similar to TestDrop(), but tests the dropping mechanism through the Migrate logic instead, to check for
// double-locking during the Drop logic.
func TestMigrateDrop(t *testing.T, m *migrate.Migrate) {
	if err := m.Drop(); err != nil {
		t.Fatal(err)
	}
}