package bigquery

import (
	"context"
	"fmt"
	"log"
	"testing"
	"time"

	bqlib "cloud.google.com/go/bigquery"
	"github.com/dhui/dktest"
	"github.com/docker/go-connections/nat"
	"github.com/golang-migrate/migrate/v4"
	dt "github.com/golang-migrate/migrate/v4/database/testing"
	"github.com/golang-migrate/migrate/v4/dktesting"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

const (
	bqProjectID    = "projectId"
	bqDataset      = "dataset"
	bqEmulatorPort = 9050
)

var (
	opts = dktest.Options{
		PortRequired: true,
		ReadyFunc:    isReady,
		ReadyTimeout: 5 * time.Second,
		Cmd:          []string{"bigquery-emulator", fmt.Sprintf("--project=%s", bqProjectID), fmt.Sprintf("--dataset=%s", bqDataset)},
		Platform:     "linux/x86_64",
		ExposedPorts: nat.PortSet{
			nat.Port(fmt.Sprintf("%d/tcp", bqEmulatorPort)): struct{}{},
		},
	}

	specs = []dktesting.ContainerSpec{
		{ImageName: "ghcr.io/goccy/bigquery-emulator:0.6.5", Options: opts},
	}
)

func isReady(ctx context.Context, c dktest.ContainerInfo) bool {
	ip, port, err := c.FirstPort()
	if err != nil {
		return false
	}

	endpoint := fmt.Sprintf("http://%s:%s", ip, port)

	client, err := bqlib.NewClient(context.Background(), bqProjectID,
		[]option.ClientOption{
			option.WithEndpoint(endpoint),
			option.WithoutAuthentication(),
		}...,
	)
	if err != nil {
		return false
	}

	query := client.Query(`SELECT 1 AS val`)

	iter, err := query.Read(context.Background())
	if err != nil {
		return false
	}

	for {
		var row struct {
			Val int
		}
		if err := iter.Next(&row); err != nil {
			switch err {
			case iterator.Done:
				return true
			default:
				log.Println(err)
			}

			return false
		}
	}
}

func Test(t *testing.T) {
	t.Run("test", test)
	t.Run("testMigrate", testMigrate)
	t.Run("testMigrationTableOption", testMigrationTableOption)

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

		endpoint := fmt.Sprintf("http://%s:%s", ip, port)
		addr := fmt.Sprintf("bigquery://%s/%s?x-insecure=true&x-endpoint=%s", bqProjectID, bqDataset, endpoint)

		bq := &BigQuery{}
		d, err := bq.Open(addr)
		if err != nil {
			t.Fatal(err)
		}

		defer func() {
			if err = d.Close(); err != nil {
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

		endpoint := fmt.Sprintf("http://%s:%s", ip, port)
		addr := fmt.Sprintf("bigquery://%s/%s?x-insecure=true&x-endpoint=%s", bqProjectID, bqDataset, endpoint)

		bq := &BigQuery{}
		d, err := bq.Open(addr)
		if err != nil {
			t.Fatal(err)
		}

		defer func() {
			if err = d.Close(); err != nil {
				t.Error(err)
			}
		}()

		m, err := migrate.NewWithDatabaseInstance("file://./examples", "bigquery", d)
		if err != nil {
			t.Fatal(err)
		}

		dt.TestMigrate(t, m)
	})
}

func testMigrationTableOption(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.FirstPort()
		if err != nil {
			t.Fatal(err)
		}

		migrationsTable := "custom_migrations_table"
		endpoint := fmt.Sprintf("http://%s:%s", ip, port)
		addr := fmt.Sprintf("bigquery://%s/%s?x-migrations-table=%s&x-insecure=true&x-endpoint=%s", bqProjectID, bqDataset, migrationsTable, endpoint)

		bq := &BigQuery{}
		d, err := bq.Open(addr)
		if err != nil {
			t.Fatal(err)
		}

		defer func() {
			if err = d.Close(); err != nil {
				t.Error(err)
			}
		}()

		m, err := migrate.NewWithDatabaseInstance("file://./examples", "bigquery", d)
		if err != nil {
			t.Fatal(err)
		}

		if err = m.Up(); err != nil {
			t.Fatal(err)
		}

		client, err := bqlib.NewClient(context.Background(), bqProjectID,
			[]option.ClientOption{
				option.WithEndpoint(endpoint),
				option.WithoutAuthentication(),
			}...,
		)
		if err != nil {
			t.Fatal(err)
		}

		query := client.Query(fmt.Sprintf("SELECT * from `projectId.dataset.%s`", migrationsTable))

		iter, err := query.Read(context.Background())
		if err != nil {
			t.Fatal(err)
		}

		for {
			var row struct {
				Version int
				Dirty   bool
			}
			if err := iter.Next(&row); err != nil {
				switch err {
				case iterator.Done:
					return
				default:
					t.Fatal(err)
				}
			}

			if row.Version != 1 {
				t.Errorf("expected next migration version to be 1, got %d", row.Version)
			}
			if row.Dirty {
				t.Errorf("expected migrations table not to be dirty")
			}
		}
	})
}
