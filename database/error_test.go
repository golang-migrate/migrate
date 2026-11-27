package database_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/golang-migrate/migrate/v4/database"
	"github.com/lib/pq"
)

func TestErrorChain(t *testing.T) {
	cause := &pq.Error{Code: "55P03"}
	wrapped := database.Error{Err: "migration failed", OrigErr: cause}
	for _, tc := range []struct {
		name string
		err  error
	}{
		{name: "value", err: wrapped},
		{name: "pointer", err: &wrapped},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := fmt.Errorf("run migration: %w", tc.err)
			if !errors.Is(err, cause) {
				t.Error("errors.Is did not find the underlying error")
			}
			var coded interface{ SQLState() string }
			if !errors.As(err, &coded) {
				t.Fatal("errors.As did not find the SQLSTATE error")
			}
			if got := coded.SQLState(); got != "55P03" {
				t.Errorf("SQLSTATE = %q, want 55P03", got)
			}
		})
	}
}
