package clickhouse_test

import (
	"context"
	"database/sql"
	sqldriver "database/sql/driver"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"testing"

	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/dhui/dktest"
	"github.com/docker/docker/api/types/mount"
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/clickhouse"
	dt "github.com/golang-migrate/migrate/v4/database/testing"
	"github.com/golang-migrate/migrate/v4/dktesting"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

const defaultPort = 9000

var (
	tableEngines = []string{"TinyLog", "MergeTree"}
	opts         = dktest.Options{
		Env:          map[string]string{"CLICKHOUSE_USER": "user", "CLICKHOUSE_PASSWORD": "password", "CLICKHOUSE_DB": "db"},
		PortRequired: true, ReadyFunc: isReady,
	}
	specs = []dktesting.ContainerSpec{
		{ImageName: "clickhouse:24.8", Options: opts},
	}
)

func clickhouseConnectionString(host, port, engine string) string {
	if engine != "" {
		return fmt.Sprintf(
			"clickhouse://%v:%v?username=user&password=password&database=db&x-multi-statement=true&x-migrations-table-engine=%v&debug=false",
			host, port, engine)
	}

	return fmt.Sprintf(
		"clickhouse://%v:%v?username=user&password=password&database=db&x-multi-statement=true&debug=false",
		host, port)
}

func isReady(ctx context.Context, c dktest.ContainerInfo) bool {
	ip, port, err := c.Port(defaultPort)
	if err != nil {
		return false
	}

	db, err := sql.Open("clickhouse", clickhouseConnectionString(ip, port, ""))

	if err != nil {
		log.Println("open error", err)
		return false
	}
	defer func() {
		if err := db.Close(); err != nil {
			log.Println("close error:", err)
		}
	}()

	if err = db.PingContext(ctx); err != nil {
		switch err {
		case sqldriver.ErrBadConn:
			return false
		default:
			fmt.Println(err)
		}
		return false
	}

	return true
}

func TestCases(t *testing.T) {
	for _, engine := range tableEngines {
		t.Run("Test_"+engine, func(t *testing.T) { testSimple(t, engine) })
		t.Run("Migrate_"+engine, func(t *testing.T) { testMigrate(t, engine) })
		t.Run("Version_"+engine, func(t *testing.T) { testVersion(t, engine) })
		t.Run("Drop_"+engine, func(t *testing.T) { testDrop(t, engine) })
	}
	t.Run("WithInstanceDefaultConfigValues", func(t *testing.T) { testSimpleWithInstanceDefaultConfigValues(t) })
}

func testSimple(t *testing.T, engine string) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.Port(defaultPort)
		if err != nil {
			t.Fatal(err)
		}

		addr := clickhouseConnectionString(ip, port, engine)
		p := &clickhouse.ClickHouse{}
		d, err := p.Open(addr)
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

func testSimpleWithInstanceDefaultConfigValues(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.Port(defaultPort)
		if err != nil {
			t.Fatal(err)
		}

		addr := clickhouseConnectionString(ip, port, "")
		conn, err := sql.Open("clickhouse", addr)
		if err != nil {
			t.Fatal(err)
		}
		d, err := clickhouse.WithInstance(conn, &clickhouse.Config{})
		if err != nil {
			_ = conn.Close()
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

func testMigrate(t *testing.T, engine string) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.Port(defaultPort)
		if err != nil {
			t.Fatal(err)
		}

		addr := clickhouseConnectionString(ip, port, engine)
		p := &clickhouse.ClickHouse{}
		d, err := p.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()
		m, err := migrate.NewWithDatabaseInstance("file://./examples/migrations", "db", d)

		if err != nil {
			t.Fatal(err)
		}
		dt.TestMigrate(t, m)
	})
}

func testVersion(t *testing.T, engine string) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		expectedVersion := 1

		ip, port, err := c.Port(defaultPort)
		if err != nil {
			t.Fatal(err)
		}

		addr := clickhouseConnectionString(ip, port, engine)
		p := &clickhouse.ClickHouse{}
		d, err := p.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()

		err = d.SetVersion(expectedVersion, false)
		if err != nil {
			t.Fatal(err)
		}

		version, _, err := d.Version()
		if err != nil {
			t.Fatal(err)
		}

		if version != expectedVersion {
			t.Fatal("Version mismatch")
		}
	})
}

func testDrop(t *testing.T, engine string) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.Port(defaultPort)
		if err != nil {
			t.Fatal(err)
		}

		addr := clickhouseConnectionString(ip, port, engine)
		p := &clickhouse.ClickHouse{}
		d, err := p.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()

		err = d.Drop()
		if err != nil {
			t.Fatal(err)
		}
	})
}

// clusterXML is a ClickHouse config that defines a single-shard cluster and an
// embedded ClickHouse Keeper so ReplicatedMergeTree tables work without a
// separate ZooKeeper process.
const clusterXML = `<clickhouse>
<remote_servers>
    <test_cluster>
        <shard>
            <replica>
                <host>localhost</host>
                <port>9000</port>
                <user>user</user>
                <password>password</password>
            </replica>
        </shard>
    </test_cluster>
</remote_servers>
<macros>
    <shard>1</shard>
    <replica>replica1</replica>
</macros>
<keeper_server>
    <tcp_port>9181</tcp_port>
    <server_id>1</server_id>
    <log_storage_path>/var/lib/clickhouse/coordination/log</log_storage_path>
    <snapshot_storage_path>/var/lib/clickhouse/coordination/snapshots</snapshot_storage_path>
    <coordination_settings>
        <operation_timeout_ms>10000</operation_timeout_ms>
        <min_session_timeout_ms>10000</min_session_timeout_ms>
        <session_timeout_ms>100000</session_timeout_ms>
    </coordination_settings>
    <raft_configuration>
        <server>
            <id>1</id>
            <hostname>localhost</hostname>
            <port>9444</port>
        </server>
    </raft_configuration>
</keeper_server>
<zookeeper>
    <node>
        <host>localhost</host>
        <port>9181</port>
    </node>
</zookeeper>
</clickhouse>`

func clickhouseClusterConnectionString(host, port string) string {
	return fmt.Sprintf(
		"clickhouse://%v:%v?username=user&password=password&database=db&x-multi-statement=true&x-cluster-name=test_cluster&x-migrations-table-engine=MergeTree&debug=false",
		host, port)
}

func clickhouseDistributedConnectionString(host, port string) string {
	return fmt.Sprintf(
		"clickhouse://%v:%v?username=user&password=password&database=db&x-multi-statement=true&x-cluster-name=test_cluster&x-distributed=true&x-migrations-table-engine=ReplicatedMergeTree&debug=false",
		host, port)
}

// withClusterContainer runs testFunc against a ClickHouse container that has a
// single-node cluster ("test_cluster") and an embedded Keeper configured.
func withClusterContainer(t *testing.T, testFunc func(*testing.T, dktest.ContainerInfo)) {
	t.Helper()

	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "cluster.xml")
	if err := os.WriteFile(cfgPath, []byte(clusterXML), 0644); err != nil {
		t.Fatal(err)
	}

	clusterOpts := dktest.Options{
		Env:          map[string]string{"CLICKHOUSE_USER": "user", "CLICKHOUSE_PASSWORD": "password", "CLICKHOUSE_DB": "db"},
		PortRequired: true,
		ReadyFunc:    isReady,
		Mounts: []mount.Mount{
			{
				Type:   mount.TypeBind,
				Source: cfgPath,
				Target: "/etc/clickhouse-server/config.d/cluster.xml",
			},
		},
	}

	clusterSpecs := []dktesting.ContainerSpec{
		{ImageName: "clickhouse:24.8", Options: clusterOpts},
	}

	dktesting.ParallelTest(t, clusterSpecs, testFunc)
}

func TestClusterCases(t *testing.T) {
	t.Run("Test_Cluster", func(t *testing.T) { testCluster(t) })
	t.Run("Drop_Cluster", func(t *testing.T) { testDropCluster(t) })
	t.Run("Test_Distributed", func(t *testing.T) { testDistributed(t) })
	t.Run("Version_Distributed", func(t *testing.T) { testVersionDistributed(t) })
}

func testCluster(t *testing.T) {
	withClusterContainer(t, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.Port(defaultPort)
		if err != nil {
			t.Fatal(err)
		}

		addr := clickhouseClusterConnectionString(ip, port)
		p := &clickhouse.ClickHouse{}
		d, err := p.Open(addr)
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

func testDropCluster(t *testing.T) {
	withClusterContainer(t, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.Port(defaultPort)
		if err != nil {
			t.Fatal(err)
		}

		addr := clickhouseClusterConnectionString(ip, port)
		p := &clickhouse.ClickHouse{}
		d, err := p.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()

		if err := d.Drop(); err != nil {
			t.Fatal(err)
		}
	})
}

func testDistributed(t *testing.T) {
	withClusterContainer(t, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.Port(defaultPort)
		if err != nil {
			t.Fatal(err)
		}

		addr := clickhouseDistributedConnectionString(ip, port)
		p := &clickhouse.ClickHouse{}
		d, err := p.Open(addr)
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

func testVersionDistributed(t *testing.T) {
	withClusterContainer(t, func(t *testing.T, c dktest.ContainerInfo) {
		expectedVersion := 1

		ip, port, err := c.Port(defaultPort)
		if err != nil {
			t.Fatal(err)
		}

		addr := clickhouseDistributedConnectionString(ip, port)
		p := &clickhouse.ClickHouse{}
		d, err := p.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()

		if err := d.SetVersion(expectedVersion, false); err != nil {
			t.Fatal(err)
		}

		version, _, err := d.Version()
		if err != nil {
			t.Fatal(err)
		}

		if version != expectedVersion {
			t.Fatal("Version mismatch")
		}
	})
}
