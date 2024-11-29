package spanner

import (
	"fmt"
	"os"
	"testing"

	"github.com/golang-migrate/migrate/v4"

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

	withSpannerEmulator(t, func(t *testing.T) {
		uri := fmt.Sprintf("spanner://%s?x-clean-statements=true", db)
		s := &Spanner{}
		d, err := s.Open(uri)
		if err != nil {
			t.Fatal(err)
		}
		dt.Test(t, d, []byte("CREATE TABLE table_name (\n  id STRING(MAX) NOT NULL,\n) PRIMARY KEY (id);\nINSERT INTO table_name (id) VALUES ('1');"))
	})
}

func TestMigrate(t *testing.T) {
	withSpannerEmulator(t, func(t *testing.T) {
		s := &Spanner{}
		uri := fmt.Sprintf("spanner://%s", db)
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

	withSpannerEmulator(t, func(t *testing.T) {
		s := &Spanner{}
		uri := fmt.Sprintf("spanner://%s?x-clean-statements=true", db)
		d, err := s.Open(uri)
		if err != nil {
			t.Fatal(err)
		}
		m, err := migrate.NewWithDatabaseInstance("file://./examples/migrations2", uri, d)
		if err != nil {
			t.Fatal(err)
		}
		dt.TestMigrate(t, m)
	})
}

func Test_statementGroups(t *testing.T) {
	testCases := []struct {
		name           string
		multiStatement string
		expected       []*statementGroup
	}{
		{
			name:           "no statement",
			multiStatement: "",
			expected:       nil,
		},
		{
			name:           "single statement, single line, no semicolon, no comment",
			multiStatement: "CREATE TABLE table_name (id STRING(255) NOT NULL) PRIMARY KEY (id)",
			expected: []*statementGroup{
				{
					typ: statementTypeDDL,
					stmts: []string{
						"CREATE TABLE table_name (id STRING(255) NOT NULL) PRIMARY KEY (id)",
					},
				},
			},
		},
		{
			name: "single statement, multi line, no semicolon, no comment",
			multiStatement: `CREATE TABLE table_name (
			id STRING(255) NOT NULL,
		) PRIMARY KEY (id)`,
			expected: []*statementGroup{
				{
					typ: statementTypeDDL,
					stmts: []string{
						"CREATE TABLE table_name (\n\t\t\tid STRING(255) NOT NULL,\n\t\t) PRIMARY KEY (id)",
					},
				},
			},
		},
		{
			name:           "single statement, single line, with semicolon, no comment",
			multiStatement: "CREATE TABLE table_name (id STRING(255) NOT NULL) PRIMARY KEY (id);",
			expected: []*statementGroup{
				{
					typ: statementTypeDDL,
					stmts: []string{
						"CREATE TABLE table_name (id STRING(255) NOT NULL) PRIMARY KEY (id)",
					},
				},
			},
		},
		{
			name: "single statement, multi line, with semicolon, no comment",
			multiStatement: `CREATE TABLE table_name (
			id STRING(255) NOT NULL,
		) PRIMARY KEY (id);`,
			expected: []*statementGroup{
				{
					typ: statementTypeDDL,
					stmts: []string{
						"CREATE TABLE table_name (\n\t\t\tid STRING(255) NOT NULL,\n\t\t) PRIMARY KEY (id)",
					},
				},
			},
		},
		{
			name: "multi statement, with trailing semicolon. no comment",
			// From https://github.com/mattes/migrate/pull/281
			multiStatement: `CREATE TABLE table_name (
			id STRING(255) NOT NULL,
		) PRIMARY KEY(id);

		CREATE INDEX table_name_id_idx ON table_name (id);`,
			expected: []*statementGroup{
				{
					typ: statementTypeDDL,
					stmts: []string{
						"CREATE TABLE table_name (\n\t\t\tid STRING(255) NOT NULL,\n\t\t) PRIMARY KEY(id)",
						"CREATE INDEX table_name_id_idx ON table_name (id)",
					},
				},
			},
		},
		{
			name: "multi statement, no trailing semicolon, no comment",
			// From https://github.com/mattes/migrate/pull/281
			multiStatement: `CREATE TABLE table_name (
			id STRING(255) NOT NULL,
		) PRIMARY KEY(id);

		CREATE INDEX table_name_id_idx ON table_name (id)`,
			expected: []*statementGroup{
				{
					typ: statementTypeDDL,
					stmts: []string{
						"CREATE TABLE table_name (\n\t\t\tid STRING(255) NOT NULL,\n\t\t) PRIMARY KEY(id)",
						"CREATE INDEX table_name_id_idx ON table_name (id)",
					},
				},
			},
		},
		{
			name: "multi statement, no trailing semicolon, standalone comment",
			// From https://github.com/mattes/migrate/pull/281
			multiStatement: `CREATE TABLE table_name (
			-- standalone comment
			id STRING(255) NOT NULL,
		) PRIMARY KEY(id);

		CREATE INDEX table_name_id_idx ON table_name (id)`,
			expected: []*statementGroup{
				{
					typ: statementTypeDDL,
					stmts: []string{
						"CREATE TABLE table_name (\n\t\t\tid STRING(255) NOT NULL,\n\t\t) PRIMARY KEY(id)",
						"CREATE INDEX table_name_id_idx ON table_name (id)",
					},
				},
			},
		},
		{
			name: "multi statement, no trailing semicolon, end-of-line comment",
			// From https://github.com/mattes/migrate/pull/281
			multiStatement: `CREATE TABLE table_name (
			id STRING(255) NOT NULL, -- end-of-line comment
		) PRIMARY KEY(id);

		CREATE INDEX table_name_id_idx ON table_name (id)`,
			expected: []*statementGroup{
				{
					typ: statementTypeDDL,
					stmts: []string{
						"CREATE TABLE table_name (\n\t\t\tid STRING(255) NOT NULL,\n\t\t) PRIMARY KEY(id)",
						"CREATE INDEX table_name_id_idx ON table_name (id)",
					},
				},
			},
		},
		{
			name: "multi statement, inline comment",
			multiStatement: `CREATE TABLE table_name (
			id STRING(255) NOT NULL, /* inline comment */
		) PRIMARY KEY(id);

		CREATE INDEX table_name_id_idx ON table_name (id);`,
			expected: []*statementGroup{
				{
					typ: statementTypeDDL,
					stmts: []string{
						"CREATE TABLE table_name (\n\t\t\tid STRING(255) NOT NULL,\n\t\t) PRIMARY KEY(id)",
						"CREATE INDEX table_name_id_idx ON table_name (id)",
					},
				},
			},
		},
		{
			name: "multi statement, inline comment inside DML",
			multiStatement: `CREATE TABLE table_name (
			id STRING(255 /* inline comment */) NOT NULL,
		) PRIMARY KEY(id);

		CREATE INDEX table_name_id_idx ON table_name (id);`,
			expected: []*statementGroup{
				{
					typ: statementTypeDDL,
					stmts: []string{
						"CREATE TABLE table_name (\n\t\t\tid STRING(255) NOT NULL,\n\t\t) PRIMARY KEY(id)",
						"CREATE INDEX table_name_id_idx ON table_name (id)",
					},
				},
			},
		},
		{
			name: "alter table with SET OPTIONS",
			multiStatement: `ALTER TABLE users ALTER COLUMN created
			SET OPTIONS (allow_commit_timestamp=true);`,
			expected: []*statementGroup{
				{
					typ: statementTypeDDL,
					stmts: []string{
						"ALTER TABLE users ALTER COLUMN created\n\t\t\tSET OPTIONS (allow_commit_timestamp=true)",
					},
				},
			},
		},
		{
			name: "column with NUMERIC type",
			multiStatement: `CREATE TABLE table_name (
				id STRING(255) NOT NULL,
				sum NUMERIC,
			) PRIMARY KEY (id)`,
			expected: []*statementGroup{
				{
					typ: statementTypeDDL,
					stmts: []string{
						"CREATE TABLE table_name (\n\t\t\t\tid STRING(255) NOT NULL,\n\t\t\t\tsum NUMERIC,\n\t\t\t) PRIMARY KEY (id)",
					},
				},
			},
		},
		{
			name:           "DML statement",
			multiStatement: "INSERT INTO table_name (id) VALUES ('1');",
			expected: []*statementGroup{
				{
					typ: statementTypeDML,
					stmts: []string{
						"INSERT INTO table_name (id) VALUES ('1')",
					},
				},
			},
		},
		{
			name:           "DDL & DML statement",
			multiStatement: "CREATE TABLE table_name (\n  id STRING(MAX) NOT NULL,\n) PRIMARY KEY (id);\nINSERT INTO table_name (id) VALUES ('1');",
			expected: []*statementGroup{
				{
					typ: statementTypeDDL,
					stmts: []string{
						"CREATE TABLE table_name (\n  id STRING(MAX) NOT NULL,\n) PRIMARY KEY (id)",
					},
				},
				{
					typ: statementTypeDML,
					stmts: []string{
						"INSERT INTO table_name (id) VALUES ('1')",
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			stmts, err := statementGroups([]byte(tc.multiStatement))
			require.NoError(t, err, "Error cleaning statements")
			assert.Equal(t, tc.expected, stmts)
		})
	}
}
