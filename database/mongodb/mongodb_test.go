package mongodb

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"

	dt "github.com/golang-migrate/migrate/v4/database/testing"
	mt "github.com/golang-migrate/migrate/v4/testing"
	"github.com/mongodb/mongo-go-driver/mongo"
)

var versions = []mt.Version{
	{Image: "mongo:4"},
	{Image: "mongo:3"},
}

func mongoConnectionString(host string, port uint) string {
	return fmt.Sprintf("mongodb://%s:%v/testMigration", host, port)
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
