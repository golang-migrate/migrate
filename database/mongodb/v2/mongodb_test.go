package mongodb

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"log"

	"io"
	"net"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
	mongodbv1 "github.com/golang-migrate/migrate/v4/database/mongodb"
)

import (
	"github.com/dhui/dktest"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/dns"
)

import (
	dt "github.com/golang-migrate/migrate/v4/database/testing"
	"github.com/golang-migrate/migrate/v4/dktesting"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

var (
	opts = dktest.Options{PortRequired: true, ReadyFunc: isReady}
	// Supported versions: https://www.mongodb.com/support-policy
	specs = []dktesting.ContainerSpec{
		{ImageName: "mongo:5.0", Options: opts},
		{ImageName: "mongo:6.0", Options: opts},
		{ImageName: "mongo:7.0", Options: opts},
		{ImageName: "mongo:8.0", Options: opts},
		{ImageName: "mongo:9.0", Options: opts},
	}
)

func mongoConnectionString(host, port string) string {
	// there is connect option for excluding serverConnection algorithm
	// it's let avoid errors with mongo replica set connection in docker container
	return fmt.Sprintf("mongodb://%s:%s/testMigration?connect=direct", host, port)
}

func isReady(ctx context.Context, c dktest.ContainerInfo) bool {
	ip, port, err := c.FirstPort()
	if err != nil {
		return false
	}

	client, err := mongo.Connect(options.Client().ApplyURI(mongoConnectionString(ip, port)))
	if err != nil {
		return false
	}
	defer func() {
		if err := client.Disconnect(ctx); err != nil {
			log.Println("close error:", err)
		}
	}()

	if err = client.Ping(ctx, nil); err != nil {
		switch err {
		case io.EOF:
			return false
		default:
			log.Println(err)
		}
		return false
	}
	return true
}

func Test(t *testing.T) {
	t.Run("test", test)
	t.Run("testMigrate", testMigrate)
	t.Run("testWithAuth", testWithAuth)
	t.Run("testLockWorks", testLockWorks)
	t.Run("testCompatibility", testCompatibility)

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

		addr := fmt.Sprintf("mongodb2://%s:%s/testMigration?connect=direct", ip, port)
		d, err := database.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()
		dt.TestNilVersion(t, d)
		dt.TestLockAndUnlock(t, d)
		dt.TestRun(t, d, bytes.NewReader([]byte(`[{"insert":"hello","documents":[{"wild":"world"}]}]`)))
		dt.TestSetVersion(t, d)
		dt.TestDrop(t, d)
	})
}

func testMigrate(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.FirstPort()
		if err != nil {
			t.Fatal(err)
		}

		addr := mongoConnectionString(ip, port)
		p := &Mongo{}
		d, err := p.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()
		m, err := migrate.NewWithDatabaseInstance("file://../examples/migrations", "", d)
		if err != nil {
			t.Fatal(err)
		}
		dt.TestMigrate(t, m)
	})
}

func testWithAuth(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.FirstPort()
		if err != nil {
			t.Fatal(err)
		}

		addr := mongoConnectionString(ip, port)
		p := &Mongo{}
		d, err := p.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()
		createUserCMD := []byte(`[{"createUser":"deminem","pwd":"gogo","roles":[{"role":"readWrite","db":"testMigration"}]}]`)
		err = d.Run(bytes.NewReader(createUserCMD))
		if err != nil {
			t.Fatal(err)
		}
		testcases := []struct {
			name            string
			connectUri      string
			isErrorExpected bool
		}{
			{"right auth data", "mongodb2://deminem:gogo@%s:%v/testMigration", false},
			{"wrong auth data", "mongodb2://wrong:auth@%s:%v/testMigration", true},
		}

		for _, tcase := range testcases {
			t.Run(tcase.name, func(t *testing.T) {
				mc := &Mongo{}
				d, err := mc.Open(fmt.Sprintf(tcase.connectUri, ip, port))
				if err == nil {
					defer func() {
						if err := d.Close(); err != nil {
							t.Error(err)
						}
					}()
				}

				switch {
				case tcase.isErrorExpected && err == nil:
					t.Fatalf("no error when expected")
				case !tcase.isErrorExpected && err != nil:
					t.Fatalf("unexpected error: %v", err)
				}
			})
		}
	})
}

func testLockWorks(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.FirstPort()
		if err != nil {
			t.Fatal(err)
		}

		addr := mongoConnectionString(ip, port)
		p := &Mongo{}
		d, err := p.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()

		dt.TestRun(t, d, bytes.NewReader([]byte(`[{"insert":"hello","documents":[{"wild":"world"}]}]`)))

		mc := d.(*Mongo)

		err = mc.Lock()
		if err != nil {
			t.Fatal(err)
		}
		err = mc.Unlock()
		if err != nil {
			t.Fatal(err)
		}

		err = mc.Lock()
		if err != nil {
			t.Fatal(err)
		}
		err = mc.Unlock()
		if err != nil {
			t.Fatal(err)
		}

		// enable locking,
		//try to hit a lock conflict
		mc.config.Locking.Enabled = true
		mc.config.Locking.Timeout = 1
		err = mc.Lock()
		if err != nil {
			t.Fatal(err)
		}
		err = mc.Lock()
		if err == nil {
			t.Fatal("should have failed, mongo should be locked already")
		}
	})
}

func testCompatibility(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.FirstPort()
		if err != nil {
			t.Fatal(err)
		}

		oldDriver, err := database.Open(mongoConnectionString(ip, port))
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := oldDriver.Close(); err != nil {
				t.Error(err)
			}
		}()
		newDriver, err := database.Open(fmt.Sprintf("mongodb2://%s:%s/testMigration?connect=direct&x-advisory-lock-timeout=1", ip, port))
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := newDriver.Close(); err != nil {
				t.Error(err)
			}
		}()

		if err := oldDriver.SetVersion(1, true); err != nil {
			t.Fatal(err)
		}
		if version, dirty, err := newDriver.Version(); err != nil || version != 1 || !dirty {
			t.Fatalf("unexpected v2 version: %d, dirty: %t, error: %v", version, dirty, err)
		}
		if err := newDriver.SetVersion(2, false); err != nil {
			t.Fatal(err)
		}
		if version, dirty, err := oldDriver.Version(); err != nil || version != 2 || dirty {
			t.Fatalf("unexpected v1 version: %d, dirty: %t, error: %v", version, dirty, err)
		}

		if err := oldDriver.Lock(); err != nil {
			t.Fatal(err)
		}
		if err := newDriver.Lock(); !errors.Is(err, database.ErrLocked) {
			t.Fatalf("expected v1 lock to block v2, got: %v", err)
		}
		if err := oldDriver.Unlock(); err != nil {
			t.Fatal(err)
		}
		if err := newDriver.Lock(); err != nil {
			t.Fatal(err)
		}
		if err := newDriver.Unlock(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestDriverRegistration(t *testing.T) {
	for _, tcase := range []struct {
		dsn string
		err error
	}{
		{"mongodb://localhost", mongodbv1.ErrNoDatabaseName},
		{"mongodb2://localhost", ErrNoDatabaseName},
	} {
		t.Run(tcase.dsn, func(t *testing.T) {
			if _, err := database.Open(tcase.dsn); !errors.Is(err, tcase.err) {
				t.Fatalf("expected %v, got %v", tcase.err, err)
			}
		})
	}
	for _, scheme := range []string{"mongodb+srv", "mongodb2+srv"} {
		t.Run(scheme, func(t *testing.T) {
			for _, driver := range database.List() {
				if driver == scheme {
					return
				}
			}
			t.Fatalf("driver %q is not registered", scheme)
		})
	}
}

func TestOpenSRV(t *testing.T) {
	resolver := dns.DefaultResolver
	dns.DefaultResolver = &dns.Resolver{
		LookupTXT: func(string) ([]string, error) {
			return nil, nil
		},
		LookupSRV: func(string, string, string) (string, []*net.SRV, error) {
			return "", []*net.SRV{{Target: "node.cluster.example.test.", Port: 27017}}, nil
		},
	}
	t.Cleanup(func() {
		dns.DefaultResolver = resolver
	})

	if _, err := database.Open("mongodb2+srv://cluster.example.test/"); !errors.Is(err, ErrNoDatabaseName) {
		t.Fatalf("expected %v, got %v", ErrNoDatabaseName, err)
	}
}

func TestTransaction(t *testing.T) {
	transactionSpecs := []dktesting.ContainerSpec{
		{ImageName: "mongo:5.0", Options: dktest.Options{PortRequired: true, ReadyFunc: isReady,
			Cmd: []string{"mongod", "--bind_ip_all", "--replSet", "rs0"}}},
	}
	t.Cleanup(func() {
		for _, spec := range transactionSpecs {
			t.Log("Cleaning up ", spec.ImageName)
			if err := spec.Cleanup(); err != nil {
				t.Error("Error removing ", spec.ImageName, "error:", err)
			}
		}
	})

	dktesting.ParallelTest(t, transactionSpecs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.FirstPort()
		if err != nil {
			t.Fatal(err)
		}

		client, err := mongo.Connect(options.Client().ApplyURI(mongoConnectionString(ip, port)))
		if err != nil {
			t.Fatal(err)
		}
		err = client.Ping(context.TODO(), nil)
		if err != nil {
			t.Fatal(err)
		}
		//rs.initiate()
		err = client.Database("admin").RunCommand(context.TODO(), bson.D{bson.E{Key: "replSetInitiate", Value: bson.D{}}}).Err()
		if err != nil {
			t.Fatal(err)
		}
		err = waitForReplicaInit(client)
		if err != nil {
			t.Fatal(err)
		}
		d, err := WithInstance(client, &Config{
			DatabaseName: "testMigration",
		})
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()
		//We have to create collection
		//transactions don't support operations with creating new dbs, collections
		//Unique index need for checking transaction aborting
		insertCMD := []byte(`[
				{"create":"hello"},
				{"createIndexes": "hello",
					"indexes": [{
						"key": {
							"wild": 1
						},
						"name": "unique_wild",
						"unique": true,
						"background": true
					}]
			}]`)
		err = d.Run(bytes.NewReader(insertCMD))
		if err != nil {
			t.Fatal(err)
		}
		testcases := []struct {
			name            string
			cmds            []byte
			documentsCount  int64
			isErrorExpected bool
		}{
			{
				name: "success transaction",
				cmds: []byte(`[{"insert":"hello","documents":[
										{"wild":"world"},
										{"wild":"west"},
										{"wild":"natural"}
									 ]
								  }]`),
				documentsCount:  3,
				isErrorExpected: false,
			},
			{
				name: "failure transaction",
				//transaction have to be failure - duplicate unique key wild:west
				//none of the documents should be added
				cmds: []byte(`[{"insert":"hello","documents":[{"wild":"flower"}]},
									{"insert":"hello","documents":[
										{"wild":"cat"},
										{"wild":"west"}
									 ]
								  }]`),
				documentsCount:  3,
				isErrorExpected: true,
			},
		}
		for _, tcase := range testcases {
			t.Run(tcase.name, func(t *testing.T) {
				client, err := mongo.Connect(options.Client().ApplyURI(mongoConnectionString(ip, port)))
				if err != nil {
					t.Fatal(err)
				}
				err = client.Ping(context.TODO(), nil)
				if err != nil {
					t.Fatal(err)
				}
				d, err := WithInstance(client, &Config{
					DatabaseName:    "testMigration",
					TransactionMode: true,
				})
				if err != nil {
					t.Fatal(err)
				}
				defer func() {
					if err := d.Close(); err != nil {
						t.Error(err)
					}
				}()
				runErr := d.Run(bytes.NewReader(tcase.cmds))
				if runErr != nil {
					if !tcase.isErrorExpected {
						t.Fatal(runErr)
					}
				}
				documentsCount, err := client.Database("testMigration").Collection("hello").CountDocuments(context.TODO(), bson.M{})
				if err != nil {
					t.Fatal(err)
				}
				if tcase.documentsCount != documentsCount {
					t.Fatalf("expected %d and actual %d documents count not equal. run migration error:%s", tcase.documentsCount, documentsCount, runErr)
				}
			})
		}
	})
}

type isMaster struct {
	IsMaster bool `bson:"ismaster"`
}

func waitForReplicaInit(client *mongo.Client) error {
	ticker := time.NewTicker(time.Second * 1)
	defer ticker.Stop()
	timeout, err := strconv.Atoi(os.Getenv("MIGRATE_TEST_MONGO_REPLICA_SET_INIT_TIMEOUT"))
	if err != nil {
		timeout = 30
	}
	timeoutTimer := time.NewTimer(time.Duration(timeout) * time.Second)
	defer timeoutTimer.Stop()
	for {
		select {
		case <-ticker.C:
			var status isMaster
			//Check that node is primary because
			//during replica set initialization, the first node first becomes a secondary and then becomes the primary
			//should consider that initialization is completed only after the node has become the primary
			result := client.Database("admin").RunCommand(context.TODO(), bson.D{bson.E{Key: "isMaster", Value: 1}})
			if err := result.Decode(&status); err != nil {
				return err
			}
			if status.IsMaster {
				return nil
			}
		case <-timeoutTimer.C:
			return fmt.Errorf("replica init timeout")
		}
	}

}
