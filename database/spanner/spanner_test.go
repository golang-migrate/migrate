package spanner

import (
	"fmt"
	"os"
	"testing"

	dt "github.com/mattes/migrate/database/testing"
)

func Test(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping testing Google Spanner during short testing")
	}

	db := os.Getenv("SPANNER_DATABASE")
	s := &Spanner{}
	addr := fmt.Sprintf("spanner://%v", db)
	d, err := s.Open(addr)
	if err != nil {
		t.Fatalf("%v", err)
	}
	dt.Test(t, d, []byte("SELECT 1"))
}
