// Package testing has the database tests.
// All database drivers must pass the Test function.
// This lives in it's own package so it stays a test dependency.
package testing

import (
	"github.com/golang-migrate/migrate/v4/database"
	"reflect"
	"testing"
)

import (
	"github.com/golang-migrate/migrate/v4"
)

// TestMigrate runs integration-tests between the Migrate layer and database implementations.
func TestMigrate(t *testing.T, m *migrate.Migrate) {
	TestMigrateUp(t, m)
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

func TestMigrateUp(t *testing.T, m *migrate.Migrate) {
	t.Log("UP")

	tt := &triggerTest{
		t: t,
		m: m,
		triggered: map[string]bool{
			migrate.TrigRunMigrationPre:         false,
			migrate.TrigRunMigrationPost:        false,
			migrate.TrigRunMigrationVersionPre:  false,
			migrate.TrigRunMigrationVersionPost: false,
			database.TrigRunPre:                 false,
			database.TrigRunPost:                false,
		},
	}

	m.Triggers = map[string]func(r migrate.TriggerResponse) error{
		migrate.TrigRunMigrationPre:         tt.trigMigrationCheck,
		migrate.TrigRunMigrationPost:        tt.trigMigrationCheck,
		migrate.TrigRunMigrationVersionPre:  tt.trigMigrationCheck,
		migrate.TrigRunMigrationVersionPost: tt.trigMigrationCheck,
	}

	m.AddDatabaseTriggers(map[string]func(response interface{}) error{
		database.TrigRunPre:  tt.trigDatabaseMigrationCheck,
		database.TrigRunPost: tt.trigDatabaseMigrationCheck,
	})

	if err := m.Up(); err != nil {
		t.Fatal(err)
	}

	if !tt.triggered[migrate.TrigRunMigrationPre] {
		t.Fatalf("expected trigger %s to be called, but it was not", migrate.TrigRunMigrationPre)
	}
	if !tt.triggered[migrate.TrigRunMigrationPost] {
		t.Fatalf("expected trigger %s to be called, but it was not", migrate.TrigRunMigrationPost)
	}
	if !tt.triggered[migrate.TrigRunMigrationVersionPre] {
		t.Fatalf("expected trigger %s to be called, but it was not", migrate.TrigRunMigrationVersionPre)
	}
	if !tt.triggered[migrate.TrigRunMigrationVersionPost] {
		t.Fatalf("expected trigger %s to be called, but it was not", migrate.TrigRunMigrationVersionPost)
	}
	if !tt.triggered[database.TrigRunPre] {
		t.Fatalf("expected database trigger %s to be called, but it was not", database.TrigRunPre)
	}
	if !tt.triggered[database.TrigRunPost] {
		t.Fatalf("expected database trigger %s to be called, but it was not", database.TrigRunPost)
	}
}

type triggerTest struct {
	t         *testing.T
	m         *migrate.Migrate
	triggered map[string]bool
}

func (tt *triggerTest) trigMigrationCheck(r migrate.TriggerResponse) error {
	tt.triggered[r.Trigger] = true
	return nil
}

func (tt *triggerTest) trigDatabaseMigrationCheck(response interface{}) error {
	val := reflect.ValueOf(response)
	field := val.FieldByName("Trigger")
	if !field.IsValid() {
		tt.t.Fatalf("expected response to have a Trigger field, got %T", response)
	}

	tt.triggered[field.String()] = true
	return nil
}
