package bigquery

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/goccy/bigquery-emulator/server"
	"github.com/goccy/bigquery-emulator/types"
	"github.com/golang-migrate/migrate/v4"

	dt "github.com/golang-migrate/migrate/v4/database/testing"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

// withBigQueryEmulator is not thread-safe and cannot be used with parallel tests since it sets the emulator
func withBigQueryEmulator(t *testing.T, testFunc func(t *testing.T, endpoint, projectID, datasetID, migrationTable string)) {
	t.Helper()
	projectID := os.Getenv("GCLOUD_PROJECT_ID")
	datasetID := "golang_migrate"
	migrationTable := fmt.Sprintf("%s_%d", DefaultMigrationsTable, time.Now().Unix())
	if projectID != "" {
		testFunc(t, "", projectID, datasetID, migrationTable)
		return
	}
	projectID = "golang-migrate"
	srv, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	err = srv.Load(server.StructSource(types.NewProject(projectID, types.NewDataset(datasetID))))
	if err != nil {
		t.Fatal(err)
	}
	err = srv.SetProject(projectID)
	if err != nil {
		t.Fatal(err)
	}
	addr := "127.0.0.1:9050"
	go func() {
		err := srv.Serve(context.Background(), addr)
		if err != nil {
			fmt.Println(err)
		}
	}()
	defer srv.Stop(context.Background())
	testFunc(t, "http://"+addr, projectID, datasetID, migrationTable)
}

func Test(t *testing.T) {
	withBigQueryEmulator(t, func(t *testing.T, endpoint, projectID, datasetID, migrationTable string) {
		db := fmt.Sprintf("projects/%s/datasets/%s", projectID, datasetID)
		uri := fmt.Sprintf("bigquery://%s?x-migrations-table=%s&x-endpoint=%s", db, migrationTable, endpoint)
		s := &BigQuery{}
		d, err := s.Open(uri)
		if err != nil {
			t.Fatal(err)
		}
		dt.Test(t, d, []byte("CREATE TABLE test (id BOOL)"))
	})
}

func TestMigrate(t *testing.T) {
	withBigQueryEmulator(t, func(t *testing.T, endpoint, projectID, datasetID, migrationTable string) {
		s := &BigQuery{}
		db := fmt.Sprintf("projects/%s/datasets/%s", projectID, datasetID)
		uri := fmt.Sprintf("bigquery://%s?x-migrations-table=%s&x-endpoint=%s", db, migrationTable, endpoint)
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
