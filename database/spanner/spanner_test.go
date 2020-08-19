package spanner

import (
	"fmt"
	"os"
	"testing"

	"github.com/golang-migrate/migrate/v4"

	dt "github.com/golang-migrate/migrate/v4/database/testing"

	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	db, ok := os.LookupEnv("SPANNER_DATABASE")
	if !ok {
		t.Skip("SPANNER_DATABASE not set, skipping test.")
	}

	s := &Spanner{}
	addr := fmt.Sprintf("spanner://%s", db)
	d, err := s.Open(addr)
	if err != nil {
		t.Fatal(err)
	}
	dt.Test(t, d, []byte("SELECT 1"))
}

func TestMigrate(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	db, ok := os.LookupEnv("SPANNER_DATABASE")
	if !ok {
		t.Skip("SPANNER_DATABASE not set, skipping test.")
	}

	s := &Spanner{}
	addr := fmt.Sprintf("spanner://%s", db)
	d, err := s.Open(addr)
	if err != nil {
		t.Fatal(err)
	}
	m, err := migrate.NewWithDatabaseInstance("file://./examples/migrations", db, d)
	if err != nil {
		t.Fatal(err)
	}
	dt.TestMigrate(t, m)
}

func TestCleanStatements(t *testing.T) {
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
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			stmts, err := cleanStatements([]byte(tc.multiStatement))
			require.NoError(t, err, "Error cleaning statements")
			assert.Equal(t, tc.expected, stmts)
		})
	}
}
