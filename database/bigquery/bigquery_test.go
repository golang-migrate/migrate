package bigquery

import (
	"context"
	"fmt"
	"log"
	"strings"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/dhui/dktest"
	"github.com/docker/go-connections/nat"
	"google.golang.org/api/option"

	"github.com/golang-migrate/migrate/v4"
	dt "github.com/golang-migrate/migrate/v4/database/testing"
	"github.com/golang-migrate/migrate/v4/dktesting"

	_ "github.com/golang-migrate/migrate/v4/source/file"
)

const (
	testProjectID = "test-project-id"
	testDatasetID = "test_dataset_id"
)

var (
	opts = dktest.Options{
		PortRequired: true,
		ReadyFunc:    isReady,
		Cmd:          []string{"--project", testProjectID, "--dataset", testDatasetID},
		Platform:     "linux/amd64",
		ExposedPorts: map[nat.Port]struct{}{
			"9050/tcp": {},
		},
	}
	specs = []dktesting.ContainerSpec{
		{ImageName: "ghcr.io/goccy/bigquery-emulator:0.6.5", Options: opts},
	}
)

func bigqueryEndpoint(ip, port string) string {
	return fmt.Sprintf("http://%s:%s/", ip, port)
}

func bigqueryConnectionString(ip, port string) string {
	options := []string{
		"x-migrations-table=schema_migrations",
		fmt.Sprintf("project_id=%s", testProjectID),
		fmt.Sprintf("dataset_id=%s", testDatasetID),
	}
	return fmt.Sprintf("bigquery://%s?%s", bigqueryEndpoint(ip, port), strings.Join(options, "&"))
}

func isReady(ctx context.Context, c dktest.ContainerInfo) bool {
	ip, port, err := c.FirstPort()
	if err != nil {
		return false
	}

	client, err := bigquery.NewClient(ctx, testProjectID,
		option.WithEndpoint(bigqueryEndpoint(ip, port)), option.WithoutAuthentication())
	if err != nil {
		return false
	}

	defer func() {
		if err := client.Close(); err != nil {
			log.Println("close error:", err)
		}
	}()

	if _, err := client.Query("SELECT 1").Read(context.Background()); err != nil {
		return false
	}

	return true
}

func Test(t *testing.T) {
	t.Run("test", test)
	t.Run("testMigrate", testMigrate)

	t.Cleanup(func() {
		for _, spec := range specs {
			t.Log("Cleaning up ", spec.ImageName)
			if err := spec.Cleanup(); err != nil {
				t.Error("Error removing ", spec.ImageName, "error:", err)
			}
		}
	})
}

func test(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.FirstPort()
		if err != nil {
			t.Fatal(err)
		}
		addr := bigqueryConnectionString(ip, port)
		bq := &BigQuery{}
		d, err := bq.Open(addr)
		if err != nil {
			t.Fatal(err)
		}

		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()

		dt.Test(t, d, []byte("SELECT 1"))
	})
}

func testMigrate(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.FirstPort()
		if err != nil {
			t.Fatal(err)
		}

		addr := bigqueryConnectionString(ip, port)
		bq := &BigQuery{}
		d, err := bq.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()

		m, err := migrate.NewWithDatabaseInstance("file://./examples/migrations", "bigquery", d)
		if err != nil {
			t.Fatal(err)
		}

		dt.TestMigrate(t, m)
	})
}
