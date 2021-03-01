package cassandra

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/dhui/dktest"
	"github.com/gocql/gocql"
	"github.com/golang-migrate/migrate/v4"

	dt "github.com/golang-migrate/migrate/v4/database/testing"
	"github.com/golang-migrate/migrate/v4/dktesting"

	_ "github.com/golang-migrate/migrate/v4/source/file"
)

var (
	opts = dktest.Options{PortRequired: true, ReadyFunc: isReady}
	// Supported versions: http://cassandra.apache.org/download/
	// Although Cassandra 2.x is supported by the Apache Foundation,
	// the migrate db driver only supports Cassandra 3.x since it uses
	// the system_schema keyspace.
	specs = []dktesting.ContainerSpec{
		{ImageName: "cassandra:3.0", Options: opts},
		{ImageName: "cassandra:3.11", Options: opts},
	}
)

func isReady(ctx context.Context, c dktest.ContainerInfo) bool {
	// Cassandra exposes 5 ports (7000, 7001, 7199, 9042 & 9160)
	// We only need the port bound to 9042
	ip, portStr, err := c.Port(9042)
	if err != nil {
		return false
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return false
	}

	cluster := gocql.NewCluster(ip)
	cluster.Port = port
	cluster.Consistency = gocql.All
	p, err := cluster.CreateSession()
	if err != nil {
		return false
	}
	defer p.Close()
	// Create keyspace for tests
	if err = p.Query("CREATE KEYSPACE testks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}").Exec(); err != nil {
		return false
	}
	return true
}

func Test(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.Port(9042)
		if err != nil {
			t.Fatal("Unable to get mapped port:", err)
		}
		addr := fmt.Sprintf("cassandra://%v:%v/testks", ip, port)
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

func TestMigrate(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.Port(9042)
		if err != nil {
			t.Fatal("Unable to get mapped port:", err)
		}
		addr := fmt.Sprintf("cassandra://%v:%v/testks", ip, port)
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

func TestSchemaVersionV2Migration(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, portStr, err := c.Port(9042)
		if err != nil {
			t.Fatal("Unable to get mapped port:", err)
		}

		port, err := strconv.Atoi(portStr)
		if err != nil {
			t.Fatal(err)
		}

		addr := fmt.Sprintf("cassandra://%v:%v/testks", ip, portStr)

		// Setup fake legacy version table
		cluster := gocql.NewCluster(ip)
		cluster.Port = port
		cluster.Keyspace = "testks"
		cluster.Consistency = gocql.All
		sess, err := cluster.CreateSession()
		if err != nil {
			t.Fatal(err)
		}

		defer sess.Close()
		if err = sess.Query("CREATE TABLE schema_migrations (version bigint, dirty boolean, PRIMARY KEY(version))").Exec(); err != nil {
			t.Fatal(err)
		}
		if err = sess.Query("INSERT INTO schema_migrations (version, dirty) VALUES (20210301171700, false)").Exec(); err != nil {
			t.Fatal(err)
		}

		// Init driver
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

		// Migration should have occurred; we need to check that the `dummy` field now exists, and that data has been preserved.
		var count int8
		err = sess.Query("SELECT COUNT(*) FROM system_schema.columns WHERE keyspace_name = 'testks' AND table_name = 'schema_migrations' AND column_name = 'dummy';").Scan(&count)
		if err != nil {
			t.Fatal(err)
		}

		if count != 1 {
			t.Error("Expected column dummy to be present in the schema_migrations table")
		}

		ver, dirty, err := d.Version()
		if err != nil {
			t.Fatal(err)
		}

		if ver != 20210301171700 || dirty {
			t.Error("Expected data to be preserved after migration")
		}
	})
}
