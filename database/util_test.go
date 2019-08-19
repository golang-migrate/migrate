package database

import (
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
