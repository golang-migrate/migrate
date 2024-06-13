package spanner

import (
	"fmt"
	"os"
	"path"
	"testing"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"

	dt "github.com/golang-migrate/migrate/v4/database/testing"
	_ "github.com/golang-migrate/migrate/v4/source/file"

	"cloud.google.com/go/spanner/spannertest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// withSpannerEmulator is not thread-safe and cannot be used with parallel tests since it sets the emulator
func withSpannerEmulator(t *testing.T, testFunc func(t *testing.T)) {
	t.Helper()
	srv, err := spannertest.NewServer("localhost:0")
	if err != nil {
		t.Fatal("Failed to create Spanner emulator:", err)
	}
	// This is not thread-safe
	if err := os.Setenv("SPANNER_EMULATOR_HOST", srv.Addr); err != nil {
		t.Fatal("Failed to set SPANNER_EMULATOR_HOST env var:", err)
	}
	defer srv.Close()
	testFunc(t)

}

const db = "projects/abc/instances/def/databases/testdb"

func Test(t *testing.T) {
	withSpannerEmulator(t, func(t *testing.T) {
		uri := fmt.Sprintf("spanner://%s", db)
		s := &Spanner{}
		d, err := s.Open(uri)
		if err != nil {
			t.Fatal(err)
		}
		dt.Test(t, d, []byte("CREATE TABLE test (id BOOL) PRIMARY KEY (id)"))
	})
}

func TestMigrate(t *testing.T) {
	testCases := []struct {
		name          string
		dsnParameters string
	}{
		{
			name: "clean statements disabled",
		},
		{
			name:          "clean statements enabled",
			dsnParameters: "x-clean-statements=true",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			withSpannerEmulator(t, func(t *testing.T) {
				s := &Spanner{}
				uri := fmt.Sprintf("spanner://%s?%s", db, tc.dsnParameters)

				d, err := s.Open(uri)
				if err != nil {
					t.Fatal(err)
				}

				m, err := migrate.NewWithDatabaseInstance("file://./examples/migrations", uri, d)
				if err != nil {
					t.Fatal(err)
				}
				dt.TestMigrate(t, m)
			})
		})
	}
}

func TestMigrateErrors(t *testing.T) {
	testCases := []struct {
		name          string
		dsnParameters string
		sql           string
		err           error
	}{
		{
			name: "impossible deletion",
			sql:  "DELETE FROM Singers WHERE FirstName = 'Alice'",
			err:  new(database.Error),
		},
		{
			name: "invalid options",
			sql:  "ALTER DATABASE `db` SET OPTIONS(foo=bar)",
			err:  new(database.Error),
		},
		{
			name:          "invalid DDL syntax",
			dsnParameters: "x-clean-statements=true",
			sql:           "This is not a DDL",
			err:           new(database.Error),
		},
	}
	for i, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			withSpannerEmulator(t, func(t *testing.T) {
				s := &Spanner{}
				uri := fmt.Sprintf("spanner://%s?%s", db, tc.dsnParameters)

				d, err := s.Open(uri)
				require.NoError(t, err)

				sourceDir := t.TempDir()
				require.NoError(t, os.WriteFile(path.Join(sourceDir, fmt.Sprintf("%d_%s.up.sql", i, tc.name)), []byte(tc.sql), 06444))

				m, err := migrate.NewWithDatabaseInstance(fmt.Sprintf("file://%s", sourceDir), uri, d)
				require.NoError(t, err)

				err = m.Up()
				require.ErrorAs(t, err, &tc.err)
			})
		})
	}
}

func TestCleanDDLStatements(t *testing.T) {
	testCases := []struct {
		name           string
		multiStatement string
		expected       []string
	}{
		{
			name:           "no statement",
			multiStatement: "",
			expected:       []string{},
		},
		{
			name:           "single statement, single line, no semicolon, no comment",
			multiStatement: "CREATE TABLE table_name (id STRING(255) NOT NULL) PRIMARY KEY (id)",
			expected:       []string{"CREATE TABLE table_name (\n  id STRING(255) NOT NULL,\n) PRIMARY KEY(id)"},
		},
		{
			name: "single statement, multi line, no semicolon, no comment",
			multiStatement: `CREATE TABLE table_name (
			id STRING(255) NOT NULL,
		) PRIMARY KEY (id)`,
			expected: []string{"CREATE TABLE table_name (\n  id STRING(255) NOT NULL,\n) PRIMARY KEY(id)"},
		},
		{
			name:           "single statement, single line, with semicolon, no comment",
			multiStatement: "CREATE TABLE table_name (id STRING(255) NOT NULL) PRIMARY KEY (id);",
			expected:       []string{"CREATE TABLE table_name (\n  id STRING(255) NOT NULL,\n) PRIMARY KEY(id)"},
		},
		{
			name: "single statement, multi line, with semicolon, no comment",
			multiStatement: `CREATE TABLE table_name (
			id STRING(255) NOT NULL,
		) PRIMARY KEY (id);`,
			expected: []string{"CREATE TABLE table_name (\n  id STRING(255) NOT NULL,\n) PRIMARY KEY(id)"},
		},
		{
			name: "multi statement, with trailing semicolon. no comment",
			// From https://github.com/mattes/migrate/pull/281
			multiStatement: `CREATE TABLE table_name (
			id STRING(255) NOT NULL,
		) PRIMARY KEY(id);

		CREATE INDEX table_name_id_idx ON table_name (id);`,
			expected: []string{`CREATE TABLE table_name (
  id STRING(255) NOT NULL,
) PRIMARY KEY(id)`, "CREATE INDEX table_name_id_idx ON table_name(id)"},
		},
		{
			name: "multi statement, no trailing semicolon, no comment",
			// From https://github.com/mattes/migrate/pull/281
			multiStatement: `CREATE TABLE table_name (
			id STRING(255) NOT NULL,
		) PRIMARY KEY(id);

		CREATE INDEX table_name_id_idx ON table_name (id)`,
			expected: []string{`CREATE TABLE table_name (
  id STRING(255) NOT NULL,
) PRIMARY KEY(id)`, "CREATE INDEX table_name_id_idx ON table_name(id)"},
		},
		{
			name: "multi statement, no trailing semicolon, standalone comment",
			// From https://github.com/mattes/migrate/pull/281
			multiStatement: `CREATE TABLE table_name (
			-- standalone comment
			id STRING(255) NOT NULL,
		) PRIMARY KEY(id);

		CREATE INDEX table_name_id_idx ON table_name (id)`,
			expected: []string{`CREATE TABLE table_name (
  id STRING(255) NOT NULL,
) PRIMARY KEY(id)`, "CREATE INDEX table_name_id_idx ON table_name(id)"},
		},
		{
			name: "multi statement, no trailing semicolon, inline comment",
			// From https://github.com/mattes/migrate/pull/281
			multiStatement: `CREATE TABLE table_name (
			id STRING(255) NOT NULL, -- inline comment
		) PRIMARY KEY(id);

		CREATE INDEX table_name_id_idx ON table_name (id)`,
			expected: []string{`CREATE TABLE table_name (
  id STRING(255) NOT NULL,
) PRIMARY KEY(id)`, "CREATE INDEX table_name_id_idx ON table_name(id)"},
		},
		{
			name: "alter table with SET OPTIONS",
			multiStatement: `ALTER TABLE users ALTER COLUMN created
			SET OPTIONS (allow_commit_timestamp=true);`,
			expected: []string{"ALTER TABLE users ALTER COLUMN created SET OPTIONS (allow_commit_timestamp = true)"},
		},
		{
			name: "column with NUMERIC type",
			multiStatement: `CREATE TABLE table_name (
				id STRING(255) NOT NULL,
				sum NUMERIC,
			) PRIMARY KEY (id)`,
			expected: []string{"CREATE TABLE table_name (\n  id STRING(255) NOT NULL,\n  sum NUMERIC,\n) PRIMARY KEY(id)"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			stmts, err := cleanDDLStatements([]byte(tc.multiStatement))
			require.NoError(t, err, "Error cleaning DDL statements")
			assert.Equal(t, tc.expected, stmts)
		})
	}
}

func TestCleanDMLStatements(t *testing.T) {
	const (
		insertStmt       = `INSERT Singers (SingerId, FirstName, LastName) VALUES (1, 'Marc', 'Richards')`
		insertParsedStmt = `INSERT INTO Singers (SingerId, FirstName, LastName) VALUES (1, "Marc", "Richards")`
		updateStmt       = `UPDATE Singers SET FirstName = "Marcel" WHERE SingerId = 1`
	)

	testCases := []struct {
		name           string
		multiStatement string
		expected       []string
	}{
		{
			name:           "no statement",
			multiStatement: "",
			expected:       []string{},
		},
		{
			name:           "single statement, single line, no semicolon, no comment",
			multiStatement: updateStmt,
			expected:       []string{updateStmt},
		},
		{
			name: "single statement, multi line, no semicolon, no comment",
			multiStatement: `UPDATE Singers
			SET FirstName = "Marcel"
			WHERE SingerId = 1
			`,
			expected: []string{updateStmt},
		},
		{
			name:           "single statement, single line, with semicolon, no comment",
			multiStatement: updateStmt + ";",
			expected:       []string{updateStmt},
		},
		{
			name: "single statement, multi line, with semicolon, no comment",
			multiStatement: `UPDATE Singers
			SET FirstName = "Marcel"
			WHERE SingerId = 1
			;`,
			expected: []string{updateStmt},
		},
		{
			name:           "multi statement, with trailing semicolon. no comment",
			multiStatement: insertStmt + ";" + updateStmt + ";",
			expected:       []string{insertParsedStmt, updateStmt},
		},
		{
			name: "multi statement, no trailing semicolon, no comment",
			// From https://github.com/mattes/migrate/pull/281
			multiStatement: insertStmt + ";" + updateStmt,
			expected:       []string{insertParsedStmt, updateStmt},
		},
		{
			name: "multi statement, no trailing semicolon, standalone comment",
			// From https://github.com/mattes/migrate/pull/281
			multiStatement: `UPDATE Singers
			-- standalone comment
			SET FirstName = "Marcel"
			WHERE SingerId = 1;` + insertStmt,
			expected: []string{updateStmt, insertParsedStmt},
		},
		{
			name: "multi statement, no trailing semicolon, inline comment",
			multiStatement: `UPDATE Singers -- inline comment
			SET FirstName = "Marcel"
			WHERE SingerId = 1;` + insertStmt,
			expected: []string{updateStmt, insertParsedStmt},
		},
		{
			name:           "delete statement",
			multiStatement: "DELETE FROM Singers WHERE FirstName = 'Alice'",
			expected:       []string{`DELETE FROM Singers WHERE FirstName = "Alice"`},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			stmts, err := cleanDMLStatements([]byte(tc.multiStatement))
			require.NoError(t, err, "Error cleaning DML statements")
			assert.Equal(t, tc.expected, stmts)
		})
	}
}

func TestCleanDMLStatementsError(t *testing.T) {
	stmts, err := cleanDMLStatements([]byte("ALTER DATABASE `db` SET OPTIONS(enable_key_visualizer=true)"))
	assert.ErrorAs(t, err, new(*dmlCleanError))
	assert.Equal(t, err.Error(), "Fail to clean DML migration statements, error: :1.0: unknown DML statement")
	assert.Empty(t, stmts)
}
