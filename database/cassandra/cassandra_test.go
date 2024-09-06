package cassandra

import (
	"context"
	"fmt"
	"testing"

	"github.com/gocql/gocql"
	"github.com/golang-migrate/migrate/v4"

	dt "github.com/golang-migrate/migrate/v4/database/testing"

	"github.com/testcontainers/testcontainers-go/modules/cassandra"

	_ "github.com/golang-migrate/migrate/v4/source/file"
)

var (
	// Supported versions: http://cassandra.apache.org/download/
	// Although Cassandra 2.x is supported by the Apache Foundation,
	// the migrate db driver only supports Cassandra 3.x since it uses
	// the system_schema keyspace.
	// last ScyllaDB version tested is 5.1.11
	specs = []string{
		"cassandra:3.0",
		"cassandra:3.11",
		"scylladb/scylla:5.1.11",
	}
)

type CreateKeySpaceCommand struct{}

func Test(t *testing.T) {
	t.Run("test", test)
	t.Run("testMigrate", testMigrate)
}

func startCassandra(spec string) (*cassandra.CassandraContainer, error) {
	c, err := cassandra.Run(context.Background(), spec)
	if err != nil {
		return nil, err
	}

	// from here, return the container and the error, so the client code
	// can defer the container.Terminate call

	// create a keyspace for the tests

	connectionHost, err := c.ConnectionHost(context.Background())
	if err != nil {
		return c, fmt.Errorf("failed to get connection host: %s", err)
	}

	cluster := gocql.NewCluster(connectionHost)
	session, err := cluster.CreateSession()
	if err != nil {
		return c, fmt.Errorf("failed to create session: %s", err)
	}
	defer session.Close()
	// Create keyspace for tests
	if err = session.Query("CREATE KEYSPACE testks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor':1}").Exec(); err != nil {
		return c, fmt.Errorf("failed to create keyspace: %s", err)
	}

	return c, nil
}

func test(t *testing.T) {
	for _, spec := range specs {
		spec := spec // capture range variable, see https://goo.gl/60w3p2
		t.Run(spec, func(t *testing.T) {
			t.Parallel()

			c, err := startCassandra(spec)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if c == nil {
					return
				}
				if err := c.Terminate(context.Background()); err != nil {
					t.Error(err)
				}
			})

			hostPort, err := c.ConnectionHost(context.Background())
			if err != nil {
				t.Fatal(err)
			}

			addr := fmt.Sprintf("cassandra://%s/testks", hostPort)
			p := &Cassandra{}
			d, err := p.Open(addr)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := d.Close(); err != nil {
					t.Error(err)
				}
			}()
			dt.Test(t, d, []byte("SELECT table_name from system_schema.tables"))
		})
	}
}

func testMigrate(t *testing.T) {
	for _, spec := range specs {
		spec := spec // capture range variable, see https://goo.gl/60w3p2
		t.Run(spec, func(t *testing.T) {
			t.Parallel()

			c, err := startCassandra(spec)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if c == nil {
					return
				}
				if err := c.Terminate(context.Background()); err != nil {
					t.Error(err)
				}
			})

			hostPort, err := c.ConnectionHost(context.Background())
			if err != nil {
				t.Fatal(err)
			}

			addr := fmt.Sprintf("cassandra://%s/testks", hostPort)
			p := &Cassandra{}
			d, err := p.Open(addr)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := d.Close(); err != nil {
					t.Error(err)
				}
			}()

			m, err := migrate.NewWithDatabaseInstance("file://./examples/migrations", "testks", d)
			if err != nil {
				t.Fatal(err)
			}
			dt.TestMigrate(t, m)
		})
	}
}
