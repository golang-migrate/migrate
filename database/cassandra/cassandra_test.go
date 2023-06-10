package cassandra

import (
	"fmt"
	"os"
	"strconv"
	"testing"

	"github.com/gocql/gocql"
	"github.com/golang-migrate/migrate/v4"
	"github.com/stretchr/testify/assert"

	dt "github.com/golang-migrate/migrate/v4/database/testing"

	_ "github.com/golang-migrate/migrate/v4/source/file"
)

type TestMode string

const (
	SimpleTest    TestMode = "simple"
	MigrationTest TestMode = "migration"
)

func TestCasandraTable(t *testing.T) {
	Cassandra3_Host := os.Getenv("CASSANDRA3_HOST")
	Cassandra3_Port := os.Getenv("CASSANDRA3_PORT")

	Cassandra3_11_Host := os.Getenv("CASSANDRA3.11_HOST")
	Cassandra3_11_Port := os.Getenv("CASSANDRA3.11_PORT")

	cases := []struct {
		description string
		host        string
		port        string
		mode        TestMode
	}{
		{
			description: "Test Cassandra 3",
			host:        Cassandra3_Host,
			port:        Cassandra3_Port,
			mode:        SimpleTest,
		},
		{
			description: "Test Cassandra 3",
			host:        Cassandra3_Host,
			port:        Cassandra3_Port,
			mode:        MigrationTest,
		},
		{
			description: "Test Cassandra 3.11",
			host:        Cassandra3_11_Host,
			port:        "9043",
			mode:        SimpleTest,
		},
		{
			description: "Test Cassandra 3.11",
			host:        Cassandra3_11_Host,
			port:        "9043",
			mode:        MigrationTest,
		},
	}

	for _, tt := range cases {
		t.Run(tt.description, func(t *testing.T) {
			ip, port := getHostAndPort()

			portAsInt, portAsIntErr := strconv.Atoi(port)
			assert.NoError(t, portAsIntErr, "could not convert port to int")
			isReady(t, ip, portAsInt)
			defer cleanUp(t, ip, portAsInt)

			p := &Cassandra{}
			d, err := p.Open(fmt.Sprintf("cassandra://%v:%v/testks", ip, port))
			assert.NoError(t, err, "could not open cassandra instance")

			defer func() {
				assert.NoError(t, d.Close(), "could not close  cassandra instance")
			}()

			if tt.mode == SimpleTest {
				dt.Test(t, d, []byte("SELECT table_name from system_schema.tables"))
			} else {
				m, err := migrate.NewWithDatabaseInstance("file://./examples/migrations", "testks", d)
				if err != nil {
					t.Fatal(err)
				}
				dt.TestMigrate(t, m)
			}
		})
	}
}

func getHostAndPort() (string, string) {
	ip := os.Getenv("CASANDRA_HOST")
	port := os.Getenv("CASANDRA_PORT")

	if ip == "" && port == "" {
		ip = "localhost"
		port = "9042"
	}
	return ip, port
}

func createCassandaClusterSession(t *testing.T, ip string, port int) *gocql.Session {
	cluster := gocql.NewCluster(ip)
	cluster.Port = port
	cluster.Consistency = gocql.All
	p, err := cluster.CreateSession()
	assert.NoError(t, err, "could not create gocql cassana session")
	return p
}

func cleanUp(t *testing.T, ip string, port int) {
	p := createCassandaClusterSession(t, ip, port)

	defer p.Close()
	dropKeyspaceErr := p.Query("DROP KEYSPACE IF EXISTS testks;").Exec()
	assert.NoError(t, dropKeyspaceErr, "could not drop cassandra keyspace testks")
}

func isReady(t *testing.T, ip string, port int) {
	p := createCassandaClusterSession(t, ip, port)

	defer p.Close()
	createKeyspaceErr := p.Query("CREATE KEYSPACE testks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor':1}").Exec()
	assert.NoError(t, createKeyspaceErr, "could not drop cassandra keyspace testks")
}
