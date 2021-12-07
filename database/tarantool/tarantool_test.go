package tarantool

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/dhui/dktest"
	"github.com/golang-migrate/migrate/v4"
	mt "github.com/golang-migrate/migrate/v4/database/testing"
	"github.com/golang-migrate/migrate/v4/dktesting"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/tarantool/go-tarantool"
)

func isReady(ctx context.Context, ci dktest.ContainerInfo) bool {
	ip, port, err := ci.FirstPort()
	if err != nil {
		return false
	}
	opts := tarantool.Opts{
		Reconnect:     1 * time.Second,
		MaxReconnects: 1,
		User:          "guest",
		Pass:          "",
	}

	client, err := tarantool.Connect(ip+":"+port, opts) // listen: '3301'
	if err != nil {
		return false
	}
	defer func() {
		if err := client.Close(); err != nil {
			return
		}
	}()

	_, err = client.Ping()
	if err != nil {
		return false
	}

	if _, err := client.Call("box.schema.space.create", []interface{}{
		"tester",
		map[string]bool{"if_not_exists": true},
	}); err != nil {
		// tarantool go client catches an encoding error,
		// but the code works without any consequences -> go lint error avoid
	}

	if _, err = client.Call("box.space.tester:format", [][]map[string]string{
		{
			{"name": "id", "type": "unsigned"},
			{"name": "name", "type": "string"},
		}}); err != nil {
		return false
	}

	if _, err := client.Call("box.space.tester:create_index", []interface{}{
		"primary",
		map[string]interface{}{
			"type":          "tree",
			"parts":         []string{"id"},
			"if_not_exists": true},
	}); err != nil {
		return false
	}

	if _, err = client.Call("box.space.tester:insert", []interface{}{
		[]interface{}{1, "John"},
	}); err != nil {
		return false
	}

	return true
}

func Test(t *testing.T) {
	dktesting.ParallelTest(t, []dktesting.ContainerSpec{{ImageName: "tarantool/tarantool",
		Options: dktest.Options{PortRequired: true, ReadyFunc: isReady}}}, func(t *testing.T, ci dktest.ContainerInfo) {
		ip, port, err := ci.FirstPort()
		if err != nil {
			t.Fatal(err)
		}
		addr := fmt.Sprintf("tarantool://%s:%s/tester", ip, port)
		tr := &Tarantool{}
		driver, err := tr.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := driver.Close(); err != nil {
				t.Error(err)
			}
		}()
		mt.Test(t, driver, []byte("box.space.tester:select"))
	})
}

func TestMigrate(t *testing.T) {
	dktesting.ParallelTest(t, []dktesting.ContainerSpec{{ImageName: "tarantool/tarantool",
		Options: dktest.Options{PortRequired: true, ReadyFunc: isReady}}}, func(t *testing.T, ci dktest.ContainerInfo) {
		ip, port, err := ci.FirstPort()
		if err != nil {
			t.Fatal("Unable to get map port: ", err)
		}
		addr := fmt.Sprintf("tarantool://%s:%s/tester", ip, port)
		tr := &Tarantool{}
		driver, err := tr.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := driver.Close(); err != nil {
				t.Error(err)
			}
		}()

		m, err := migrate.NewWithDatabaseInstance("file://./examples/migrations", "tester", driver)
		if err != nil {
			t.Fatal(err)
		}
		mt.TestMigrate(t, m)
	})
}
