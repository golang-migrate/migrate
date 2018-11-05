package database

import (
	"testing"
)

func TestGenerateAdvisoryLockId(t *testing.T) {
	testcases := []struct {
		dbname     string
		schema     string
		expectedID string // empty string signifies that an error is expected
	}{
		{dbname: "database_name", expectedID: "1764327054"},
		{dbname: "database_name", schema: "schema_name_1", expectedID: "3244152297"},
		{dbname: "database_name", schema: "schema_name_2", expectedID: "810103531"},
	}

	for _, tc := range testcases {
		t.Run(tc.dbname, func(t *testing.T) {
			names := []string{}
			if len(tc.schema) > 0 {
				names = append(names, tc.schema)
			}
			if id, err := GenerateAdvisoryLockId(tc.dbname, names...); err == nil {
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
