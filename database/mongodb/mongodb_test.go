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

			driverWithAuth, err := p.Open(fmt.Sprintf("mongodb://deminem:gogo@%s:%v/testMigration", i.Host(), i.Port()))
			if err != nil {
				t.Fatalf("%v", err)
			}
			defer driverWithAuth.Close()
			insertCMD := []byte(`[{"insert":"hello","documents":[{"wild":"world"}]}]`)
			err = driverWithAuth.Run(bytes.NewReader(insertCMD))
			if err != nil {
				t.Fatalf("%v", err)
			}

			driverWithWrongAuth, err := p.Open(fmt.Sprintf("mongodb://wrong:auth@%s:%v/testMigration", i.Host(), i.Port()))
			if err != nil {
				t.Fatalf("%v", err)
			}
			defer driverWithWrongAuth.Close()
			err = driverWithWrongAuth.Run(bytes.NewReader(insertCMD))
			if err == nil {
				t.Fatal("no error with wrong authorization")
			}
		})
}
