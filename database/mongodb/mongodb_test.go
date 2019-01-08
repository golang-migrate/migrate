package mongodb

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"testing"
	"time"

	dt "github.com/golang-migrate/migrate/v4/database/testing"
	mt "github.com/golang-migrate/migrate/v4/testing"
	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/mongodb/mongo-go-driver/mongo"
)

var versions = []mt.Version{
	{Image: "mongo:4"},
	{Image: "mongo:3"},
}

func mongoConnectionString(host string, port uint) string {
	//there is connect option for excluding serverConnection algorithm
	//it's let avoid errors with mongo replica set connection in docker container
	return fmt.Sprintf("mongodb://%s:%v/testMigration?connect=single", host, port)
}

func isReady(i mt.Instance) bool {
	client, err := mongo.Connect(context.TODO(), mongoConnectionString(i.Host(), i.Port()))
	if err != nil {
		return false
	}
	defer client.Disconnect(context.TODO())
	if err = client.Ping(context.TODO(), nil); err != nil {
		switch err {
		case io.EOF:
			return false
		default:
			fmt.Println(err)
		}
		return false
	}
	return true
}

func Test(t *testing.T) {
	mt.ParallelTest(t, versions, isReady,
		func(t *testing.T, i mt.Instance) {
			p := &Mongo{}
			addr := mongoConnectionString(i.Host(), i.Port())
			d, err := p.Open(addr)
			if err != nil {
				t.Fatalf("%v", err)
			}
			defer d.Close()
			dt.TestNilVersion(t, d)
			//TestLockAndUnlock(t, d) driver doesn't support lock on database level
			dt.TestRun(t, d, bytes.NewReader([]byte(`[{"insert":"hello","documents":[{"wild":"world"}]}]`)))
			dt.TestSetVersion(t, d)
			dt.TestDrop(t, d)
		})
}

func TestWithAuth(t *testing.T) {
	mt.ParallelTest(t, versions, isReady,
		func(t *testing.T, i mt.Instance) {
			p := &Mongo{}
			addr := mongoConnectionString(i.Host(), i.Port())
			d, err := p.Open(addr)
			if err != nil {
				t.Fatalf("%v", err)
			}
			defer d.Close()
			createUserCMD := []byte(`[{"createUser":"deminem","pwd":"gogo","roles":[{"role":"readWrite","db":"testMigration"}]}]`)
			err = d.Run(bytes.NewReader(createUserCMD))
			if err != nil {
				t.Fatalf("%v", err)
			}
			testcases := []struct {
				name            string
				connectUri      string
				isErrorExpected bool
			}{
				{"right auth data", "mongodb://deminem:gogo@%s:%v/testMigration", false},
				{"wrong auth data", "mongodb://wrong:auth@%s:%v/testMigration", true},
			}
			insertCMD := []byte(`[{"insert":"hello","documents":[{"wild":"world"}]}]`)

			for _, tcase := range testcases {
				//With wrong authenticate `Open` func doesn't return auth error
				//Because at the moment golang mongo driver doesn't support auth during connection
				//For getting auth error we should execute database command
				t.Run(tcase.name, func(t *testing.T) {
					mc := &Mongo{}
					d, err := mc.Open(fmt.Sprintf(tcase.connectUri, i.Host(), i.Port()))
					if err != nil {
						t.Fatalf("%v", err)
					}
					defer d.Close()
					err = d.Run(bytes.NewReader(insertCMD))
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

func TestTransaction(t *testing.T) {
	versionsSupportedTransactions := []mt.Version{
		{
			Image: "mongo:4",
			Cmd: []string{
				"mongod",
				"--bind_ip_all",
				"--replSet", "rs0",
			}},
	}
	mt.ParallelTest(t, versionsSupportedTransactions, isReady,
		func(t *testing.T, i mt.Instance) {
			client, err := mongo.Connect(context.TODO(), mongoConnectionString(i.Host(), i.Port()))
			if err != nil {
				t.Fatalf("%v", err)
			}
			err = client.Ping(context.TODO(), nil)
			if err != nil {
				t.Fatalf("%v", err)
			}
			//rs.initiate()
			err = client.Database("admin").RunCommand(context.TODO(), bson.D{{"replSetInitiate", bson.D{}}}).Err()
			if err != nil {
				t.Fatalf("%v", err)
			}
			err = waitForReplicaInit(client)
			if err != nil {
				t.Fatalf("%v", err)
			}
			d, err := WithInstance(client, &Config{
				DatabaseName: "testMigration",
			})
			defer d.Close()
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
				t.Fatalf("%v", err)
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
					client, err := mongo.Connect(context.TODO(), mongoConnectionString(i.Host(), i.Port()))
					if err != nil {
						t.Fatalf("%v", err)
					}
					err = client.Ping(context.TODO(), nil)
					if err != nil {
						t.Fatalf("%v", err)
					}
					d, err := WithInstance(client, &Config{
						DatabaseName:    "testMigration",
						TransactionMode: true,
					})
					if err != nil {
						t.Fatalf("%v", err)
					}
					defer d.Close()
					runErr := d.Run(bytes.NewReader(tcase.cmds))
					if runErr != nil {
						if !tcase.isErrorExpected {
							t.Fatalf("%v", runErr)
						}
					}
					documentsCount, err := client.Database("testMigration").Collection("hello").Count(context.TODO(), nil)
					if err != nil {
						t.Fatalf("%v", err)
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
			result := client.Database("admin").RunCommand(context.TODO(), bson.D{{"isMaster", 1}})
			r, err := result.DecodeBytes()
			if err != nil {
				return err
			}
			err = bson.Unmarshal(r, &status)
			if err != nil {
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
