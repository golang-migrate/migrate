package database

import (
	"go.uber.org/atomic"
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

func TestCasRestoreOnErr(t *testing.T) {
	testcases := []struct {
		name        string
		lock        *atomic.Bool
		from        bool
		to          bool
		casErr      error
		fErr        error
		expectLock  bool
		expectError error
	}{
		{
			name:        "Test positive CAS lock",
			lock:        atomic.NewBool(false),
			from:        false,
			to:          true,
			casErr:      ErrLocked,
			fErr:        nil,
			expectError: nil,
			expectLock:  true,
		},
		{
			name:        "Test negative CAS lock",
			lock:        atomic.NewBool(true),
			from:        false,
			to:          true,
			casErr:      ErrLocked,
			fErr:        nil,
			expectLock:  true,
			expectError: ErrLocked,
		},
		{
			name:        "Test negative with callback lock",
			lock:        atomic.NewBool(false),
			from:        false,
			to:          true,
			casErr:      ErrLocked,
			fErr:        ErrNotLocked,
			expectLock:  false,
			expectError: ErrNotLocked,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			err := CasRestoreOnErr(tc.lock, tc.from, tc.to, tc.casErr, func() error {
				return tc.fErr
			})

			if tc.lock.Load() != tc.expectLock {
				t.Error("Incorrect state of lock")
			}

			if err != tc.expectError {
				t.Error("Incorrect error value returned")
			}
		})
	}
}
