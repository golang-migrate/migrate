package multistmt_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/getoutreach/migrate/v4/database/multistmt"
)

const maxMigrationSize = 1024

func TestParse(t *testing.T) {

	plpgsqlBody := `CREATE OR REPLACE function fn1() returns TRIGGER as $$
	-- this is a function body
	DECLARE
	BEGIN
	SELECT parent_path || flagship_role_id::text FROM roles WHERE id = NEW.parent_id INTO path;
	IF path IS NULL THEN
		RAISE EXCEPTION 'Invalid role parent %', NEW.parent_id;
	END IF;
	END;
	$$ LANGUAGE plpgsql;`

	testCases := []struct {
		name        string
		multiStmt   string
		delimiter   string
		expected    []string
		expectedErr error
	}{
		//// these tests are changed from their original, why would we expect to add a missing delimiter?
		{name: "single statement, no delimiter", multiStmt: "single statement, no delimiter", delimiter: ";",
			expected: []string{}, expectedErr: nil},
		{name: "single statement, one delimiter", multiStmt: "single statement, one delimiter;", delimiter: ";",
			expected: []string{"single statement, one delimiter;"}, expectedErr: nil},
		
		{name: "two statements, no trailing delimiter", multiStmt: "statement one; statement two", delimiter: ";",
			expected: []string{"statement one;"}, expectedErr: nil},
		{name: "two statements, with trailing delimiter", multiStmt: "statement one; statement two;", delimiter: ";",
			expected: []string{"statement one;", " statement two;"}, expectedErr: nil},
		{name: "multi line plpgsql body", multiStmt: plpgsqlBody, delimiter: "$$.*;",
			expected: []string{plpgsqlBody}, expectedErr: nil},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			stmts := make([]string, 0, len(tc.expected))
			err := multistmt.Parse(strings.NewReader(tc.multiStmt), []byte(tc.delimiter), maxMigrationSize, func(b []byte) bool {
				stmts = append(stmts, string(b))
				return true
			})
			assert.Equal(t, tc.expectedErr, err)
			assert.Equal(t, tc.expected, stmts)
		})
	}
}

func TestParseDiscontinue(t *testing.T) {
	multiStmt := "statement one; statement two"
	delimiter := ";"
	expected := []string{"statement one;"}

	stmts := make([]string, 0, len(expected))
	err := multistmt.Parse(strings.NewReader(multiStmt), []byte(delimiter), maxMigrationSize, func(b []byte) bool {
		stmts = append(stmts, string(b))
		return false
	})
	assert.Nil(t, err)
	assert.Equal(t, expected, stmts)
}
