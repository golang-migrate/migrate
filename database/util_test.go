package database

import (
	"testing"
)

func TestGenerateAdvisoryLockId(t *testing.T) {
	testcases := []struct {
		dbname     string
		expectedID string // empty string signifies that an error is expected
	}{
		{dbname: "database_name", expectedID: "1764327054"},
	}

	for _, tc := range testcases {
		t.Run(tc.dbname, func(t *testing.T) {
			if id, err := GenerateAdvisoryLockId("database_name"); err == nil {
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
