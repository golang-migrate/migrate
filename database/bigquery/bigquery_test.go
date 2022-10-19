package bigquery

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"

	dt "github.com/golang-migrate/migrate/v4/database/testing"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

// withBigQueryEmulator is not thread-safe and cannot be used with parallel tests since it sets the emulator
func withBigQueryEmulator(t *testing.T, testFunc func(t *testing.T, projectID, datasetID, migrationTable string)) {
	t.Helper()
	projectID := os.Getenv("GCLOUD_PROJECT_ID")
	datasetID := "golang_migrate"
	migrationTable := fmt.Sprintf("%s_%d", DefaultMigrationsTable, time.Now().Unix())
	if projectID == "" {		
		t.Fatalf("missing google cloud project id (GCLOUD_PROJECT_ID)")
	}
	testFunc(t, projectID, datasetID, migrationTable)
}

func Test(t *testing.T) {
	withBigQueryEmulator(t, func(t *testing.T, projectID, datasetID, migrationTable string) {
		db := fmt.Sprintf("projects/%s/datasets/%s", projectID, datasetID)
		uri := fmt.Sprintf("bigquery://%s?x-migrations-table=%s", db, migrationTable)
		s := &BigQuery{}
		d, err := s.Open(uri)
		if err != nil {
			t.Fatal(err)
		}
		dt.Test(t, d, []byte("CREATE TABLE test (id BOOL)"))
	})
}

func TestMigrate(t *testing.T) {
	withBigQueryEmulator(t, func(t *testing.T, projectID, datasetID, migrationTable string) {
		s := &BigQuery{}
		db := fmt.Sprintf("projects/%s/datasets/%s", projectID, datasetID)
		uri := fmt.Sprintf("bigquery://%s?x-migrations-table=%s", db, migrationTable)
		d, err := s.Open(uri)
		if err != nil {
			t.Fatal(err)
		}
		m, err := migrate.NewWithDatabaseInstance("file://./examples/migrations", uri, d)
		if err != nil {
			t.Fatal(err)
		}
		dt.TestMigrate(t, m)
	})
}
