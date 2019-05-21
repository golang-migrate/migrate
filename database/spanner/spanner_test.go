package spanner

import (
	"fmt"
	"os"
	"testing"
)

import (
	"github.com/golang-migrate/migrate/v4"
	dt "github.com/golang-migrate/migrate/v4/database/testing"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

import (
	"github.com/stretchr/testify/assert"
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
	dt.TestMigrate(t, m, []byte("SELECT 1"))
}

func TestMultistatementSplit(t *testing.T) {
	testCases := []struct {
		name           string
		multiStatement string
		expected       []string
	}{
		{
			name:           "single statement, single line, no semicolon",
			multiStatement: "CREATE TABLE table_name (id STRING(255) NOT NULL) PRIMARY KEY (id)",
			expected:       []string{"CREATE TABLE table_name (id STRING(255) NOT NULL) PRIMARY KEY (id)"},
		},
		{
			name: "single statement, multi line, no semicolon",
			multiStatement: `CREATE TABLE table_name (
	id STRING(255) NOT NULL,
) PRIMARY KEY (id)`,
			expected: []string{`CREATE TABLE table_name (
	id STRING(255) NOT NULL,
) PRIMARY KEY (id)`},
		},
		{
			name:           "single statement, single line, with semicolon",
			multiStatement: "CREATE TABLE table_name (id STRING(255) NOT NULL) PRIMARY KEY (id);",
			expected:       []string{"CREATE TABLE table_name (id STRING(255) NOT NULL) PRIMARY KEY (id)"},
		},
		{
			name: "single statement, multi line, with semicolon",
			multiStatement: `CREATE TABLE table_name (
	id STRING(255) NOT NULL,
) PRIMARY KEY (id);`,
			expected: []string{`CREATE TABLE table_name (
	id STRING(255) NOT NULL,
) PRIMARY KEY (id)`},
		},
		{
			name: "multi statement, with trailing semicolon",
			// From https://github.com/mattes/migrate/pull/281
			multiStatement: `CREATE TABLE table_name (
	id STRING(255) NOT NULL,
) PRIMARY KEY(id);

CREATE INDEX table_name_id_idx ON table_name (id);`,
			expected: []string{`CREATE TABLE table_name (
	id STRING(255) NOT NULL,
) PRIMARY KEY(id)`, "\n\nCREATE INDEX table_name_id_idx ON table_name (id)"},
		},
		{
			name: "multi statement, no trailing semicolon",
			// From https://github.com/mattes/migrate/pull/281
			multiStatement: `CREATE TABLE table_name (
	id STRING(255) NOT NULL,
) PRIMARY KEY(id);

CREATE INDEX table_name_id_idx ON table_name (id)`,
			expected: []string{`CREATE TABLE table_name (
	id STRING(255) NOT NULL,
) PRIMARY KEY(id)`, "\n\nCREATE INDEX table_name_id_idx ON table_name (id)"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if stmts := migrationStatements([]byte(tc.multiStatement)); !assert.Equal(t, stmts, tc.expected) {
				t.Error()
			}
		})
	}
}
