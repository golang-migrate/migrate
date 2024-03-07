package rqlite

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/dhui/dktest"
	"github.com/rqlite/gorqlite"
	"github.com/stretchr/testify/assert"

	"github.com/golang-migrate/migrate/v4"
	dt "github.com/golang-migrate/migrate/v4/database/testing"
	"github.com/golang-migrate/migrate/v4/dktesting"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

var defaultPort uint16 = 4001

var opts = dktest.Options{
	Env:          map[string]string{"NODE_ID": "1"},
	PortRequired: true,
	ReadyFunc:    isReady,
}
var specs = []dktesting.ContainerSpec{
	{ImageName: "rqlite/rqlite:7.21.4", Options: opts},
	{ImageName: "rqlite/rqlite:8.0.6", Options: opts},
	{ImageName: "rqlite/rqlite:8.11.1", Options: opts},
	{ImageName: "rqlite/rqlite:8.12.3", Options: opts},
}

func isReady(ctx context.Context, c dktest.ContainerInfo) bool {
	ip, port, err := c.Port(defaultPort)
	if err != nil {
		fmt.Println("error getting port")
		return false
	}

	statusString := fmt.Sprintf("http://%s:%s/status", ip, port)
	fmt.Println(statusString)

	var readyResp struct {
		Store struct {
			Ready bool `json:"ready"`
		} `json:"store"`
	}

	resp, err := http.Get(statusString)
	if err != nil {
		fmt.Println("error getting status")
		return false
	}

	if resp.StatusCode != 200 {
		fmt.Println("statusCode != 200")
		return false
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("error reading body")
		return false
	}

	if err := json.Unmarshal(body, &readyResp); err != nil {
		fmt.Println("error unmarshaling body")
		return false
	}

	return readyResp.Store.Ready
}

func Test(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.Port(defaultPort)
		assert.NoError(t, err)

		connectString := fmt.Sprintf("rqlite://%s:%s?level=strong&disableClusterDiscovery=true&x-connect-insecure=true", ip, port)
		t.Logf("DB connect string : %s\n", connectString)

		r := &Rqlite{}
		d, err := r.Open(connectString)
		assert.NoError(t, err)

		dt.Test(t, d, []byte("CREATE TABLE t (Qty int, Name string);"))
	})
}

func TestMigrate(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.Port(defaultPort)
		assert.NoError(t, err)

		connectString := fmt.Sprintf("rqlite://%s:%s?level=strong&disableClusterDiscovery=true&x-connect-insecure=true", ip, port)
		t.Logf("DB connect string : %s\n", connectString)

		driver, err := OpenURL(connectString)
		assert.NoError(t, err)
		defer func() {
			if err := driver.Close(); err != nil {
				return
			}
		}()

		m, err := migrate.NewWithDatabaseInstance(
			"file://./examples/migrations",
			"ql", driver)
		assert.NoError(t, err)

		dt.TestMigrate(t, m)
	})
}

func TestBadConnectInsecureParam(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.Port(defaultPort)
		assert.NoError(t, err)

		connectString := fmt.Sprintf("rqlite://%s:%s?x-connect-insecure=foo", ip, port)
		t.Logf("DB connect string : %s\n", connectString)

		_, err = OpenURL(connectString)
		assert.ErrorIs(t, err, ErrBadConfig)
	})
}

func TestBadProtocol(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.Port(defaultPort)
		assert.NoError(t, err)

		connectString := fmt.Sprintf("postgres://%s:%s/database", ip, port)
		t.Logf("DB connect string : %s\n", connectString)

		_, err = OpenURL(connectString)
		assert.ErrorIs(t, err, ErrBadConfig)
	})
}

func TestNoConfig(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.Port(defaultPort)
		assert.NoError(t, err)

		// gorqlite expects http(s) schemes
		connectString := fmt.Sprintf("http://%s:%s?level=strong&disableClusterDiscovery=true", ip, port)
		t.Logf("DB connect string : %s\n", connectString)
		db, err := gorqlite.Open(connectString)
		assert.NoError(t, err)

		_, err = WithInstance(db, nil)
		assert.ErrorIs(t, err, ErrNilConfig)
	})
}

func TestWithInstanceEmptyConfig(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.Port(defaultPort)
		assert.NoError(t, err)

		// gorqlite expects http(s) schemes
		connectString := fmt.Sprintf("http://%s:%s?level=strong&disableClusterDiscovery=true", ip, port)
		t.Logf("DB connect string : %s\n", connectString)
		db, err := gorqlite.Open(connectString)
		assert.NoError(t, err)

		driver, err := WithInstance(db, &Config{})
		assert.NoError(t, err)

		defer func() {
			if err := driver.Close(); err != nil {
				t.Fatal(err)
			}
		}()

		m, err := migrate.NewWithDatabaseInstance(
			"file://./examples/migrations",
			"ql", driver)
		assert.NoError(t, err)

		t.Log("UP")
		err = m.Up()
		assert.NoError(t, err)

		_, err = db.QueryOne(fmt.Sprintf("SELECT * FROM %s", DefaultMigrationsTable))
		assert.NoError(t, err)

		t.Log("DOWN")
		err = m.Down()
		assert.NoError(t, err)
	})
}

func TestMigrationTable(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.Port(defaultPort)
		assert.NoError(t, err)

		// gorqlite expects http(s) schemes
		connectString := fmt.Sprintf("http://%s:%s?level=strong&disableClusterDiscovery=true", ip, port)
		t.Logf("DB connect string : %s\n", connectString)
		db, err := gorqlite.Open(connectString)
		assert.NoError(t, err)

		config := Config{MigrationsTable: "my_migration_table"}
		driver, err := WithInstance(db, &config)
		assert.NoError(t, err)

		defer func() {
			if err := driver.Close(); err != nil {
				t.Fatal(err)
			}
		}()

		m, err := migrate.NewWithDatabaseInstance(
			"file://./examples/migrations",
			"ql", driver)
		assert.NoError(t, err)

		t.Log("UP")
		err = m.Up()
		assert.NoError(t, err)

		_, err = db.QueryOne(fmt.Sprintf("SELECT * FROM %s", config.MigrationsTable))
		assert.NoError(t, err)

		_, err = db.WriteOne(`INSERT INTO pets (name, predator) VALUES ("franklin", true)`)
		assert.NoError(t, err)

		res, err := db.QueryOne(`SELECT name, predator FROM pets LIMIT 1`)
		assert.NoError(t, err)

		_ = res.Next()

		// make sure we can use the migrated table
		var petName string
		var petPredator int
		err = res.Scan(&petName, &petPredator)
		assert.NoError(t, err)
		assert.Equal(t, petName, "franklin")
		assert.Equal(t, petPredator, 1)

		t.Log("DOWN")
		err = m.Down()
		assert.NoError(t, err)

		_, err = db.QueryOne(fmt.Sprintf("SELECT * FROM %s", config.MigrationsTable))
		assert.NoError(t, err)
	})
}

func TestParseUrl(t *testing.T) {
	tests := []struct {
		name           string
		passedUrl      string
		expectedUrl    string
		expectedConfig *Config
		expectedErr    string
	}{
		{
			"defaults",
			"rqlite://localhost:4001",
			"https://localhost:4001",
			&Config{ConnectInsecure: DefaultConnectInsecure, MigrationsTable: DefaultMigrationsTable},
			"",
		},
		{
			"configure migration table",
			"rqlite://localhost:4001?x-migrations-table=foo",
			"https://localhost:4001",
			&Config{ConnectInsecure: DefaultConnectInsecure, MigrationsTable: "foo"},
			"",
		},
		{
			"configure connect insecure",
			"rqlite://localhost:4001?x-connect-insecure=true",
			"http://localhost:4001",
			&Config{ConnectInsecure: true, MigrationsTable: DefaultMigrationsTable},
			"",
		},
		{
			"invalid migration table",
			"rqlite://localhost:4001?x-migrations-table=sqlite_bar",
			"",
			nil,
			"invalid value for x-migrations-table: bad parameter",
		},
		{
			"invalid connect insecure",
			"rqlite://localhost:4001?x-connect-insecure=baz",
			"",
			nil,
			"invalid value for x-connect-insecure: bad parameter",
		},
		{
			"invalid url",
			string([]byte{0x7f}),
			"",
			nil,
			"parse \"\\x7f\": net/url: invalid control character in URL",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actualUrl, actualConfig, actualErr := parseUrl(tt.passedUrl)
			if tt.expectedUrl != "" {
				assert.Equal(t, tt.expectedUrl, actualUrl.String())
			} else {
				assert.Nil(t, actualUrl)
			}

			assert.Equal(t, tt.expectedConfig, actualConfig)

			if tt.expectedErr == "" {
				assert.NoError(t, actualErr)
			} else {
				assert.EqualError(t, actualErr, tt.expectedErr)
			}
		})
	}
}
