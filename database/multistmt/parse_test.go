package multistmt_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/golang-migrate/migrate/v4/database/multistmt"
)

const maxMigrationSize = 1024

func TestParse(t *testing.T) {
	testCases := []struct {
		name        string
		multiStmt   string
		delimiter   string
		expected    []string
		expectedErr error
	}{
		{name: "single statement, no delimiter", multiStmt: "single statement, no delimiter", delimiter: ";",
			expected: []string{"single statement, no delimiter"}, expectedErr: nil},
		{name: "single statement, one delimiter", multiStmt: "single statement, one delimiter;", delimiter: ";",
			expected: []string{"single statement, one delimiter;"}, expectedErr: nil},
		{name: "two statements, no trailing delimiter", multiStmt: "statement one; statement two", delimiter: ";",
			expected: []string{"statement one;", " statement two"}, expectedErr: nil},
		{name: "two statements, with trailing delimiter", multiStmt: "statement one; statement two;", delimiter: ";",
			expected: []string{"statement one;", " statement two;"}, expectedErr: nil},
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
