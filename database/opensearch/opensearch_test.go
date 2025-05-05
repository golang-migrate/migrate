package opensearch

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/dhui/dktest"
	"github.com/golang-migrate/migrate/v4"
	dt "github.com/golang-migrate/migrate/v4/database/testing"
	"github.com/golang-migrate/migrate/v4/dktesting"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/opensearch-project/opensearch-go"
)

const DefaultPort = 9200

var (
	opts = dktest.Options{
		Env: map[string]string{
			"discovery.type":               "single-node",
			"OPENSEARCH_SECURITY_DISABLED": "true",
			"DISABLE_INSTALL_DEMO_CONFIG":  "true",
			"plugins.security.disabled":    "true",
		},
		PortRequired: true,
		ReadyFunc:    isReady,
	}
	specs = []dktesting.ContainerSpec{
		{ImageName: "opensearchproject/opensearch:1.3.19", Options: opts},
		{ImageName: "opensearchproject/opensearch:2.17.0", Options: opts},
	}
)

// getOpenSearchClient returns a new OpenSearch client.
func getOpenSearchClient(ip, port string) (*opensearch.Client, error) {
	cfg := opensearch.Config{
		Addresses: []string{fmt.Sprintf("http://%s:%s", ip, port)},
	}

	client, err := opensearch.NewClient(cfg)
	if err != nil {
		return nil, err
	}

	return client, nil
}

// isReady checks if the OpenSearch container is ready.
func isReady(ctx context.Context, c dktest.ContainerInfo) bool {
	ip, port, err := c.Port(DefaultPort)
	if err != nil {
		return false
	}

	client, err := getOpenSearchClient(ip, port)
	if err != nil {
		return false
	}

	res, err := client.Cluster.Health(
		client.Cluster.Health.WithWaitForStatus("yellow"),
		client.Cluster.Health.WithTimeout(1*time.Second),
	)
	if err == nil && res.StatusCode == 200 {
		defer func() {
			if err := res.Body.Close(); err != nil {
				fmt.Printf("failed to close response body: %v\n", err)
			}
		}()
		return true
	}

	return false
}

func TestOpenSearch(t *testing.T) {
	t.Run("testRun", testRun)
	t.Run("testMigrate", testMigrate)
	t.Run("testWithInstance", testWithInstance)

	t.Cleanup(func() {
		for _, spec := range specs {
			t.Log("cleaning up ", spec.ImageName)
			if err := spec.Cleanup(); err != nil {
				t.Error("error removing ", spec.ImageName, "error:", err)
			}
		}
	})
}

func testRun(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.Port(DefaultPort)
		if err != nil {
			t.Fatal("unable to get mapped port:", err)
		}

		addr := fmt.Sprintf("opensearch://%s:%s/migrations", ip, port)
		p := &OpenSearch{}

		d, err := p.Open(addr)
		if err != nil {
			t.Fatal("failed to open driver:", err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error("failed to close driver:", err)
			}
		}()

		migrationScript := []byte(`{
			"action": "PUT /test",
			"body": {
				"settings": {
					"number_of_shards": 1,
					"number_of_replicas": 0
				}
			}
		}`)
		if err := d.Run(bytes.NewReader(migrationScript)); err != nil {
			t.Fatal("failed to run migration:", err)
		}

		client, err := getOpenSearchClient(ip, port)
		if err != nil {
			t.Fatal("failed to create OpenSearch client:", err)
		}

		res, err := client.Indices.Exists([]string{"test"})
		if err != nil {
			t.Fatal("failed to check if index exists:", err)
		}
		defer func() {
			if err := res.Body.Close(); err != nil {
				fmt.Printf("failed to close response body: %v\n", err)
			}
		}()

		if res.StatusCode != 200 {
			t.Fatalf("expected index to exists, but got status code: %d", res.StatusCode)
		}
	})
}

func testMigrate(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.Port(9200)
		if err != nil {
			t.Fatal("unable to get mapped port:", err)
		}

		addr := fmt.Sprintf("opensearch://%s:%s/migrations", ip, port)

		m, err := migrate.New("file://./examples/migrations", addr)
		if err != nil {
			t.Fatal(err)
		}

		dt.TestMigrate(t, m)
	})
}

func testWithInstance(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.Port(9200)
		if err != nil {
			t.Fatal("unable to get mapped port:", err)
		}

		client, err := getOpenSearchClient(ip, port)
		if err != nil {
			t.Fatal("failed to create OpenSearch client:", err)
		}

		p := &OpenSearch{}
		cfg := &Config{
			Index:   "migrations",
			Timeout: 1 * time.Minute,
		}

		d, err := p.WithInstance(client, cfg)
		if err != nil {
			t.Fatal("failed to create driver with instance")
		}

		m, err := migrate.NewWithDatabaseInstance("file://./examples/migrations", "migrations", d)
		if err != nil {
			t.Fatal(err)
		}

		dt.TestMigrate(t, m)
	})
}
