package migrate

import (
	"testing"

	dStub "github.com/golang-migrate/migrate/v4/database/stub"
	"github.com/golang-migrate/migrate/v4/source"
	sStub "github.com/golang-migrate/migrate/v4/source/stub"
)

// newMigrateWithContent creates a Migrate instance backed by stub drivers.
// The single up migration at version 1 has the given content as its body.
func newMigrateWithContent(t *testing.T, content string) (*Migrate, *dStub.Stub) {
	t.Helper()

	migrations := source.NewMigrations()
	migrations.Append(&source.Migration{Version: 1, Direction: source.Up, Identifier: content})

	m, err := New("stub://", "stub://")
	if err != nil {
		t.Fatal(err)
	}
	m.sourceDrv.(*sStub.Stub).Migrations = migrations

	dbDrv := m.databaseDrv.(*dStub.Stub)
	return m, dbDrv
}

// TestStatementDelimiterSplitsFragments verifies that when StatementDelimiter is
// set, each fragment between delimiter lines is passed to the driver as a
// separate Run call.
func TestStatementDelimiterSplitsFragments(t *testing.T) {
	const content = "stmt1;\n---\nstmt2;"
	m, dbDrv := newMigrateWithContent(t, content)
	m.StatementDelimiter = []byte("\n---\n")

	if err := m.Up(); err != nil {
		t.Fatal(err)
	}

	// multistmt.Parse includes the delimiter bytes in the first fragment.
	want := []string{"stmt1;\n---\n", "stmt2;"}
	if !dbDrv.EqualSequence(want) {
		t.Errorf("MigrationSequence = %q, want %q", dbDrv.MigrationSequence, want)
	}
}

// TestStatementDelimiterDollarQuotedBody verifies that a migration containing a
// DO $$ ... $$; block (which contains semicolons) is split correctly when a
// newline-anchored delimiter is used, so the inner semicolons are never split on.
func TestStatementDelimiterDollarQuotedBody(t *testing.T) {
	const content = "CREATE TABLE foo (id INT);\n---\nDO $$ BEGIN RAISE NOTICE 'hi'; END $$;"
	m, dbDrv := newMigrateWithContent(t, content)
	m.StatementDelimiter = []byte("\n---\n")

	if err := m.Up(); err != nil {
		t.Fatal(err)
	}

	want := []string{
		"CREATE TABLE foo (id INT);\n---\n",
		"DO $$ BEGIN RAISE NOTICE 'hi'; END $$;",
	}
	if !dbDrv.EqualSequence(want) {
		t.Errorf("MigrationSequence = %q, want %q", dbDrv.MigrationSequence, want)
	}
}

// TestStatementDelimiterNilPreservesExistingBehavior verifies that when
// StatementDelimiter is nil (the default), the migration body is passed to the
// driver in a single Run call with its content unchanged.
func TestStatementDelimiterNilPreservesExistingBehavior(t *testing.T) {
	const content = "stmt1; stmt2;"
	m, dbDrv := newMigrateWithContent(t, content)
	// StatementDelimiter is nil by default — no split should occur.

	if err := m.Up(); err != nil {
		t.Fatal(err)
	}

	want := []string{content}
	if !dbDrv.EqualSequence(want) {
		t.Errorf("MigrationSequence = %q, want %q", dbDrv.MigrationSequence, want)
	}
}

// TestStatementDelimiterAbsentInContent verifies that when the delimiter is set
// but does not appear in the migration body, the entire body is passed as a
// single statement.
func TestStatementDelimiterAbsentInContent(t *testing.T) {
	const content = "CREATE TABLE bar (id INT);"
	m, dbDrv := newMigrateWithContent(t, content)
	m.StatementDelimiter = []byte("\n---\n")

	if err := m.Up(); err != nil {
		t.Fatal(err)
	}

	want := []string{content}
	if !dbDrv.EqualSequence(want) {
		t.Errorf("MigrationSequence = %q, want %q", dbDrv.MigrationSequence, want)
	}
}
