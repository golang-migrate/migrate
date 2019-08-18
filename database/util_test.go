package database

import (
	"errors"
	"testing"
)

func TestGenerateAdvisoryLockId(t *testing.T) {
	testcases := []struct {
		dbname     string
		additional []string
		expectedID string // empty string signifies that an error is expected
	}{
		{
			dbname:     "database_name",
			expectedID: "1764327054",
		},
		{
			dbname:     "database_name",
			additional: []string{"schema_name_1"},
			expectedID: "2453313553",
		},
		{
			dbname:     "database_name",
			additional: []string{"schema_name_2"},
			expectedID: "235207038",
		},
		{
			dbname:     "database_name",
			additional: []string{"schema_name_1", "schema_name_2"},
			expectedID: "3743845847",
		},
	}

	for _, tc := range testcases {
		t.Run(tc.dbname, func(t *testing.T) {
			if id, err := GenerateAdvisoryLockId(tc.dbname, tc.additional...); err == nil {
				if id != tc.expectedID {
					t.Error("Generated incorrect ID:", id, "!=", tc.expectedID)
				}
			} else {
				if tc.expectedID != "" {
					t.Error("Got unexpected error:", err)
				}
			}
		})
	}
}

func TestSchemeFromUrlSuccess(t *testing.T) {
	cases := []struct {
		name     string
		urlStr   string
		expected string
	}{
		{
			name:     "Simple",
			urlStr:   "protocol://path",
			expected: "protocol",
		},
		{
			// See issue #264
			name:     "MySQLWithPort",
			urlStr:   "mysql://user:pass@tcp(host:1337)/db",
			expected: "mysql",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			u, err := SchemeFromURL(tc.urlStr)
			if err != nil {
				t.Fatalf("expected no error, but received %q", err)
			}
			if u != tc.expected {
				t.Fatalf("expected %q, but received %q", tc.expected, u)
			}
		})
	}
}

func TestSchemeFromUrlFailure(t *testing.T) {
	cases := []struct {
		name      string
		urlStr    string
		expectErr error
	}{
		{
			name:      "Empty",
			urlStr:    "",
			expectErr: errors.New("URL cannot be empty"),
		},
		{
			name:      "NoScheme",
			urlStr:    "hello",
			expectErr: errors.New("no scheme"),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := SchemeFromURL(tc.urlStr)
			if err.Error() != tc.expectErr.Error() {
				t.Fatalf("expected %q, but received %q", tc.expectErr, err)
			}
		})
	}
}
