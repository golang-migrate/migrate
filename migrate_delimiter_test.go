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

// TestMigrationSplitterSplitsIntoSteps verifies that when MigrationSplitter is
// set, each step between delimiter lines is passed to the driver as a
// separate Run call.
func TestMigrationSplitterSplitsIntoSteps(t *testing.T) {
	const content = "stmt1;\n---\nstmt2;"
	m, dbDrv := newMigrateWithContent(t, content)
	m.MigrationSplitter = []byte("---")

	if err := m.Up(); err != nil {
		t.Fatal(err)
	}

	want := []string{"stmt1;", "stmt2;"}
	if !dbDrv.EqualSequence(want) {
		t.Errorf("MigrationSequence = %q, want %q", dbDrv.MigrationSequence, want)
	}
}

// TestMigrationSplitterDollarQuotedBody verifies that a migration containing a
// DO $$ ... $$; block (which contains semicolons) is split correctly when a
// newline-anchored delimiter is used, so the inner semicolons are never split on.
func TestMigrationSplitterDollarQuotedBody(t *testing.T) {
	const content = "CREATE TABLE foo (id INT);\n---\nDO $$ BEGIN RAISE NOTICE 'hi'; END $$;"
	m, dbDrv := newMigrateWithContent(t, content)
	m.MigrationSplitter = []byte("---")

	if err := m.Up(); err != nil {
		t.Fatal(err)
	}

	want := []string{
		"CREATE TABLE foo (id INT);",
		"DO $$ BEGIN RAISE NOTICE 'hi'; END $$;",
	}
	if !dbDrv.EqualSequence(want) {
		t.Errorf("MigrationSequence = %q, want %q", dbDrv.MigrationSequence, want)
	}
}

// TestMigrationSplitterNilPreservesExistingBehavior verifies that when
// MigrationSplitter is nil (the default), the migration body is passed to the
// driver in a single Run call with its content unchanged.
func TestMigrationSplitterNilPreservesExistingBehavior(t *testing.T) {
	const content = "stmt1; stmt2;"
	m, dbDrv := newMigrateWithContent(t, content)
	// MigrationSplitter is nil by default — no split should occur.

	if err := m.Up(); err != nil {
		t.Fatal(err)
	}

	want := []string{content}
	if !dbDrv.EqualSequence(want) {
		t.Errorf("MigrationSequence = %q, want %q", dbDrv.MigrationSequence, want)
	}
}

// TestMigrationSplitterAbsentInContent verifies that when the splitter is set
// but does not appear in the migration body, the entire body is passed as a
// single statement.
func TestMigrationSplitterAbsentInContent(t *testing.T) {
	const content = "CREATE TABLE bar (id INT);"
	m, dbDrv := newMigrateWithContent(t, content)
	m.MigrationSplitter = []byte("---")

	if err := m.Up(); err != nil {
		t.Fatal(err)
	}

	want := []string{content}
	if !dbDrv.EqualSequence(want) {
		t.Errorf("MigrationSequence = %q, want %q", dbDrv.MigrationSequence, want)
	}
}

// TestMigrationSplitterTrailingDelimiterSkipsEmptyStep verifies that when a
// migration ends with the splitter delimiter, the final empty split fragment
// is not executed.
func TestMigrationSplitterTrailingDelimiterSkipsEmptyStep(t *testing.T) {
	const content = "stmt1;\n---\nstmt2;\n---\n"
	m, dbDrv := newMigrateWithContent(t, content)
	m.MigrationSplitter = []byte("---")

	if err := m.Up(); err != nil {
		t.Fatal(err)
	}

	want := []string{"stmt1;", "stmt2;"}
	if !dbDrv.EqualSequence(want) {
		t.Errorf("MigrationSequence = %q, want %q", dbDrv.MigrationSequence, want)
	}
}

// TestMigrationSplitterEmptyPreservesExistingBehavior verifies that an empty
// splitter is treated as disabled and the full migration body is executed once.
func TestMigrationSplitterEmptyPreservesExistingBehavior(t *testing.T) {
	const content = "stmt1; stmt2;"
	m, dbDrv := newMigrateWithContent(t, content)
	m.MigrationSplitter = []byte{}

	if err := m.Up(); err != nil {
		t.Fatal(err)
	}

	want := []string{content}
	if !dbDrv.EqualSequence(want) {
		t.Errorf("MigrationSequence = %q, want %q", dbDrv.MigrationSequence, want)
	}
}

// TestMigrationSplitterSkipsWhitespaceOnlySteps verifies that fragments
// containing only whitespace between splitter lines are not executed.
func TestMigrationSplitterSkipsWhitespaceOnlySteps(t *testing.T) {
	const content = "stmt1;\n---\n \t \r\n---\nstmt2;"
	m, dbDrv := newMigrateWithContent(t, content)
	m.MigrationSplitter = []byte("---")

	if err := m.Up(); err != nil {
		t.Fatal(err)
	}

	want := []string{"stmt1;", "stmt2;"}
	if !dbDrv.EqualSequence(want) {
		t.Errorf("MigrationSequence = %q, want %q", dbDrv.MigrationSequence, want)
	}
}

// TestMigrationSplitterSupportsCRLF verifies that splitter lines are detected
// in CRLF-formatted files.
func TestMigrationSplitterSupportsCRLF(t *testing.T) {
	const content = "stmt1;\r\n---\r\nstmt2;"
	m, dbDrv := newMigrateWithContent(t, content)
	m.MigrationSplitter = []byte("---")

	if err := m.Up(); err != nil {
		t.Fatal(err)
	}

	want := []string{"stmt1;", "stmt2;"}
	if !dbDrv.EqualSequence(want) {
		t.Errorf("MigrationSequence = %q, want %q", dbDrv.MigrationSequence, want)
	}
}

// TestMigrationSplitterAtFileBoundaries verifies splitter lines are recognized
// at both the start and end of a file.
func TestMigrationSplitterAtFileBoundaries(t *testing.T) {
	const content = "---\nstmt1;\n---\nstmt2;\n---"
	m, dbDrv := newMigrateWithContent(t, content)
	m.MigrationSplitter = []byte("---")

	if err := m.Up(); err != nil {
		t.Fatal(err)
	}

	want := []string{"stmt1;", "stmt2;"}
	if !dbDrv.EqualSequence(want) {
		t.Errorf("MigrationSequence = %q, want %q", dbDrv.MigrationSequence, want)
	}
}
