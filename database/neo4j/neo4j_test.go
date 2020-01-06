package neo4j

import (
	"fmt"
	"testing"

	"github.com/dhui/dktest"
	"github.com/golang-migrate/migrate/v4/dktesting"

	dt "github.com/golang-migrate/migrate/v4/database/testing"

)

var (
	opts = dktest.Options{PortRequired: true, ReadyFunc: nil}
	specs = []dktesting.ContainerSpec{
		{ImageName: "neo4j:3.5", Options: opts},
		{ImageName: "neo4j:3.5-enterprise", Options: opts},
	}
)

func neoConnectionString(host, port string) string {
	return fmt.Sprintf("neo4j://neo4j:neo4j@%s:%s", host, port)
}

func Test(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.FirstPort()
		if err != nil {
			t.Fatal(err)
		}

		addr := neoConnectionString(ip, port)
		n := &Neo4J{}
		d, err := n.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()
		dt.Test(t, d, []byte("MATCH a RETURN a"))
	})
}