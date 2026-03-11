package hana

import (
	"os"
	"testing"

	"github.com/golang-migrate/migrate/v4"
	dt "github.com/golang-migrate/migrate/v4/database/testing"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

const envKey = "HANA_DATABASE_URL"

func TestMigrate(t *testing.T) {
	url := getURL(t)

	p := &Hana{}
	d, err := p.Open(url)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := d.Close(); err != nil {
			t.Error(err)
		}
	}()

	m, err := migrate.NewWithDatabaseInstance("file://./examples/migrations", "hdb", d)
	if err != nil {
		t.Fatal(err)
	}

	dt.TestMigrate(t, m)
}

func getURL(t *testing.T) string {
	t.Helper()
	url := os.Getenv(envKey)
	if url == "" {
		t.Skipf("skipping integration test: %s not set", envKey)
	}

	return url
}
