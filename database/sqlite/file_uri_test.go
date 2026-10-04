package sqlite_test

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/golang-migrate/migrate/v4/database"
	_ "github.com/golang-migrate/migrate/v4/database/sqlite"
)

func TestOpenFileMemoryURI(t *testing.T) {
	for _, prefix := range []string{"sqlite://file:", "sqlite://" + filepath.Join(t.TempDir(), "control.db")} {
		t.Run(prefix, func(t *testing.T) {
			addr := prefix
			if strings.HasSuffix(prefix, "file:") {
				addr += "migrate-uri?mode=memory&cache=shared&x-migrations-table=uri_versions"
			}
			d, err := database.Open(addr)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := d.Close(); err != nil {
					t.Error(err)
				}
			}()
			if err := d.Run(strings.NewReader("CREATE TABLE uri_example (value INTEGER); INSERT INTO uri_example VALUES (42);")); err != nil {
				t.Fatal(err)
			}
			if err := d.SetVersion(17, false); err != nil {
				t.Fatal(err)
			}
			if strings.HasSuffix(prefix, "file:") {
				shared, err := sql.Open("sqlite", "file:migrate-uri?mode=memory&cache=shared")
				if err != nil {
					t.Fatal(err)
				}
				defer func() {
					if err := shared.Close(); err != nil {
						t.Error(err)
					}
				}()
				var count int
				if err := shared.QueryRow("SELECT COUNT(*) FROM uri_example").Scan(&count); err != nil || count != 1 {
					t.Fatalf("shared memory rows = %d, %v", count, err)
				}
				if err := shared.QueryRow("SELECT COUNT(*) FROM uri_versions").Scan(&count); err != nil || count != 1 {
					t.Fatalf("custom version table = %d, %v", count, err)
				}
			}
			version, dirty, err := d.Version()
			if err != nil || version != 17 || dirty {
				t.Fatalf("Version() = %d, %v, %v", version, dirty, err)
			}
			if err := d.Run(strings.NewReader(fmt.Sprintf("INSERT INTO uri_example VALUES (%d)", version))); err != nil {
				t.Fatal(err)
			}
		})
	}
}
