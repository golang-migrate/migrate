package hana

import (
	"database/sql"
	"errors"
	"testing"

	_ "github.com/golang-migrate/migrate/v4/source/file"
)

func TestOpen(t *testing.T) {
	t.Parallel()

	testcases := []struct {
		name        string
		inputURL    string
		expectedErr error
	}{
		{
			name:        "missing schema",
			inputURL:    "hdb://user:pass@localhost:443",
			expectedErr: ErrNoSchemaName,
		},
		{
			name:        "invalid statement timeout",
			inputURL:    "hdb://user:pass@localhost:443?x-migrations-schema=TEST&x-statement-timeout=INVALID",
			expectedErr: ErrInvalidStatementTimeout,
		},
		{
			name:        "invalid isolation level",
			inputURL:    "hdb://user:pass@localhost:443?x-migrations-schema=TEST&x-isolation-level=999",
			expectedErr: ErrInvalidIsolationLevel,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := (&Hana{}).Open(tc.inputURL)
			if !errors.Is(err, tc.expectedErr) {
				t.Fatalf("expected %v, got %v", tc.expectedErr, err)
			}
		})
	}
}

func TestWithInstance(t *testing.T) {
	t.Parallel()

	testcases := []struct {
		name        string
		db          *sql.DB
		config      *Config
		expectedErr error
	}{
		{
			name:        "nil config",
			db:          nil,
			config:      nil,
			expectedErr: ErrNilConfig,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := WithInstance(tc.db, tc.config)
			if !errors.Is(err, tc.expectedErr) {
				t.Fatalf("expected %v, got %v", tc.expectedErr, err)
			}
		})
	}
}
