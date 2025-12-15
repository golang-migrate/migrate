package spanner

import (
	"fmt"
	"os"
	"testing"

	"cloud.google.com/go/spanner/spannertest"
	"cloud.google.com/go/spanner/spansql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/golang-migrate/migrate/v4"
	dt "github.com/golang-migrate/migrate/v4/database/testing"
	_ "github.com/golang-migrate/migrate/v4/source/file"
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
}

func TestParseStatements(t *testing.T) {
	testCases := []struct {
		name         string
		migration    string
		expectedKind statementKind
		expectError  error
	}{
		{
			name:        "empty migration",
			migration:   "",
			expectError: ErrEmptyMigration,
		},
		{
			name:        "whitespace only migration",
			migration:   "   \n\t  ",
			expectError: ErrEmptyMigration,
		},
		{
			name:         "single DDL statement - CREATE TABLE",
			migration:    "CREATE TABLE users (id STRING(36) NOT NULL) PRIMARY KEY (id)",
			expectedKind: statementKindDDL,
		},
		{
			name: "multiple DDL statements",
			migration: `CREATE TABLE users (
				id STRING(36) NOT NULL
			) PRIMARY KEY (id);
			CREATE INDEX users_idx ON users (id);`,
			expectedKind: statementKindDDL,
		},
		{
			name:         "single DML statement - INSERT",
			migration:    "INSERT INTO users (id, name) VALUES ('1', 'test')",
			expectedKind: statementKindDML,
		},
		{
			name: "multiple INSERT statements",
			migration: `INSERT INTO users (id, name) VALUES ('1', 'test1');
			INSERT INTO users (id, name) VALUES ('2', 'test2');`,
			expectedKind: statementKindDML,
		},
		{
			name:         "single partitioned DML - UPDATE",
			migration:    "UPDATE users SET name = 'updated' WHERE id = '1'",
			expectedKind: statementKindPartitionedDML,
		},
		{
			name:         "single partitioned DML - DELETE",
			migration:    "DELETE FROM users WHERE id = '1'",
			expectedKind: statementKindPartitionedDML,
		},
		{
			name: "multiple UPDATE statements",
			migration: `UPDATE users SET name = 'updated1' WHERE id = '1';
			UPDATE users SET name = 'updated2' WHERE id = '2';`,
			expectedKind: statementKindPartitionedDML,
		},
		{
			name: "mixed UPDATE and DELETE",
			migration: `UPDATE users SET name = 'updated' WHERE id = '1';
			DELETE FROM users WHERE id = '2';`,
			expectedKind: statementKindPartitionedDML,
		},
		{
			name: "DDL with inline comment",
			migration: `CREATE TABLE users (
				id STRING(36) NOT NULL, -- primary identifier
				name STRING(100)
			) PRIMARY KEY (id)`,
			expectedKind: statementKindDDL,
		},
		{
			name: "DDL with standalone comment",
			migration: `-- This migration creates the users table
			CREATE TABLE users (
				id STRING(36) NOT NULL
			) PRIMARY KEY (id)`,
			expectedKind: statementKindDDL,
		},
		{
			name: "DDL with multi-line comment",
			migration: `/*
			 * This is a multi-line comment
			 * describing the migration
			 */
			CREATE TABLE users (
				id STRING(36) NOT NULL
			) PRIMARY KEY (id)`,
			expectedKind: statementKindDDL,
		},
		{
			name: "DML INSERT with comment",
			migration: `-- Seed initial user data
			INSERT INTO users (id, name) VALUES ('1', 'test')`,
			expectedKind: statementKindDML,
		},
		{
			name: "DML UPDATE with comment",
			migration: `-- Update user emails
			UPDATE users SET name = 'updated' WHERE id = '1'`,
			expectedKind: statementKindPartitionedDML,
		},
		{
			name: "DML DELETE with comment",
			migration: `-- Clean up test data
			DELETE FROM users WHERE id = '1'`,
			expectedKind: statementKindPartitionedDML,
		},
		{
			name: "mixed INSERT and UPDATE - should error",
			migration: `INSERT INTO users (id, name) VALUES ('1', 'test');
			UPDATE users SET name = 'updated' WHERE id = '1';`,
			expectError: ErrMixedStatements,
		},
		{
			name: "mixed INSERT and DELETE - should error",
			migration: `INSERT INTO users (id, name) VALUES ('1', 'test');
			DELETE FROM users WHERE id = '1';`,
			expectError: ErrMixedStatements,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			stmts, kind, err := parseStatements([]byte(tc.migration))
			if tc.expectError != nil {
				require.ErrorIs(t, err, tc.expectError)
				return
			}
			require.NoError(t, err, "Error parsing statements")
			assert.Equal(t, tc.expectedKind, kind)
			assert.NotEmpty(t, stmts)
		})
	}
}

func TestInspectDMLKind(t *testing.T) {
	testCases := []struct {
		name         string
		migration    string
		expectedKind statementKind
		expectError  error
	}{
		{
			name:         "INSERT only",
			migration:    "INSERT INTO users (id) VALUES ('1')",
			expectedKind: statementKindDML,
		},
		{
			name:         "UPDATE only",
			migration:    "UPDATE users SET name = 'test' WHERE id = '1'",
			expectedKind: statementKindPartitionedDML,
		},
		{
			name:         "DELETE only",
			migration:    "DELETE FROM users WHERE id = '1'",
			expectedKind: statementKindPartitionedDML,
		},
		{
			name: "multiple INSERTs",
			migration: `INSERT INTO users (id) VALUES ('1');
			INSERT INTO users (id) VALUES ('2');`,
			expectedKind: statementKindDML,
		},
		{
			name: "UPDATE and DELETE combined",
			migration: `UPDATE users SET name = 'test' WHERE id = '1';
			DELETE FROM users WHERE id = '2';`,
			expectedKind: statementKindPartitionedDML,
		},
		{
			name: "INSERT and UPDATE mixed - error",
			migration: `INSERT INTO users (id) VALUES ('1');
			UPDATE users SET name = 'test' WHERE id = '1';`,
			expectError: ErrMixedStatements,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			dml, err := spansql.ParseDML("", tc.migration)
			require.NoError(t, err, "Failed to parse DML")

			kind, err := inspectDMLKind(dml.List)
			if tc.expectError != nil {
				require.ErrorIs(t, err, tc.expectError)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.expectedKind, kind)
		})
	}
}
