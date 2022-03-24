package neo4j

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"testing"

	"github.com/dhui/dktest"
	"github.com/neo4j/neo4j-go-driver/neo4j"

	"github.com/golang-migrate/migrate/v4"
	dt "github.com/golang-migrate/migrate/v4/database/testing"
	"github.com/golang-migrate/migrate/v4/dktesting"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

var (
	opts = dktest.Options{PortRequired: true, ReadyFunc: isReady,
		Env: map[string]string{"NEO4J_AUTH": "neo4j/migratetest", "NEO4J_ACCEPT_LICENSE_AGREEMENT": "yes"}}
	specs = []dktesting.ContainerSpec{
		{ImageName: "neo4j:4.0", Options: opts},
		{ImageName: "neo4j:4.0-enterprise", Options: opts},
		{ImageName: "neo4j:3.5", Options: opts},
		{ImageName: "neo4j:3.5-enterprise", Options: opts},
	}
)

func neoConnectionString(host, port string) string {
	return fmt.Sprintf("bolt://neo4j:migratetest@%s:%s", host, port)
}

func isReady(ctx context.Context, c dktest.ContainerInfo) bool {
	ip, port, err := c.Port(7687)
	if err != nil {
		return false
	}

	driver, err := neo4j.NewDriver(
		neoConnectionString(ip, port),
		neo4j.BasicAuth("neo4j", "migratetest", ""),
		func(config *neo4j.Config) {
			config.Encrypted = false
		})
	if err != nil {
		return false
	}
	defer func() {
		if err := driver.Close(); err != nil {
			log.Println("close error:", err)
		}
	}()
	session, err := driver.Session(neo4j.AccessModeRead)
	if err != nil {
		return false
	}
	result, err := session.Run("RETURN 1", nil)
	if err != nil {
		return false
	} else if result.Err() != nil {
		return false
	}

	return true
}

func Test(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.Port(7687)
		if err != nil {
			t.Fatal(err)
		}

		n := &Neo4j{}
		d, err := n.Open(neoConnectionString(ip, port))
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()
		dt.Test(t, d, []byte("MATCH (a) RETURN a"))
	})
}

func TestMigrate(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.Port(7687)
		if err != nil {
			t.Fatal(err)
		}

		n := &Neo4j{}
		neoUrl := neoConnectionString(ip, port) + "/?x-multi-statement=true"
		d, err := n.Open(neoUrl)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()
		m, err := migrate.NewWithDatabaseInstance("file://./examples/migrations", "neo4j", d)
		if err != nil {
			t.Fatal(err)
		}
		dt.TestMigrate(t, m)
	})
}

func TestMalformed(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.Port(7687)
		if err != nil {
			t.Fatal(err)
		}

		n := &Neo4j{}
		d, err := n.Open(neoConnectionString(ip, port))
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()

		migration := bytes.NewReader([]byte("CREATE (a {qid: 1) RETURN a"))
		if err := d.Run(migration); err == nil {
			t.Fatal("expected failure for malformed migration")
		}
	})
}
