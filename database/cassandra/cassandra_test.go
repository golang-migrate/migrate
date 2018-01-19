package cassandra

import (
	"fmt"
	"testing"
	dt "github.com/golang-migrate/migrate/database/testing"
	mt "github.com/golang-migrate/migrate/testing"
	"github.com/gocql/gocql"
	"time"
	"strconv"
)

var versions = []mt.Version{
	{Image: "cassandra:3.0.10"},
	{Image: "cassandra:3.0"},
}

func isReady(i mt.Instance) bool {
	// Cassandra exposes 5 ports (7000, 7001, 7199, 9042 & 9160)
	// We only need the port bound to 9042, but we can only access to the first one
	// through 'i.Port()' (which calls DockerContainer.firstPortMapping())
	// So we need to get port mapping to retrieve correct port number bound to 9042
	portMap := i.NetworkSettings().Ports
	port, _ := strconv.Atoi(portMap["9042/tcp"][0].HostPort)

	cluster := gocql.NewCluster(i.Host())
	cluster.Port = port
	//cluster.ProtoVersion = 4
	cluster.Consistency = gocql.All
	cluster.Timeout = 1 * time.Minute
	p, err := cluster.CreateSession()
	if err != nil {
		return false
	}
	// Create keyspace for tests
	p.Query("CREATE KEYSPACE testks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor':1}").Exec()
	return true
}

func Test(t *testing.T) {
	mt.ParallelTest(t, versions, isReady,
		func(t *testing.T, i mt.Instance) {
			p := &Cassandra{}
			portMap := i.NetworkSettings().Ports
			port, _ := strconv.Atoi(portMap["9042/tcp"][0].HostPort)
			addr := fmt.Sprintf("cassandra://%v:%v/testks", i.Host(), port)
			d, err := p.Open(addr)
			if err != nil {
				t.Fatalf("%v", err)
			}
			dt.Test(t, d, []byte("SELECT table_name from system_schema.tables"))
		})
}
