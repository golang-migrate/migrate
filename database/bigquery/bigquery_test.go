package bigquery

import (
	"context"
	"errors"
	"github.com/golang-migrate/migrate/v4/database"
	"strings"
	"testing"
)

const (
	//connectionUrl = "bigquery://https://bigquery.googleapis.com/bigquery/v2/?x-migrations-table=schema_migrations&x-statement-timeout=0&credentials_filename=./tmp/myproject-XXXXXXXXXXXXX-XXXXXXXXXXXX.json&dataset_id=mydataset"
	connectionUrl = "bigquery://http://0.0.0.0:9050/?x-migrations-table=schema_migrations&project_id=myproject&dataset_id=mydataset"
)

func openConnection() (database.Driver, error) {
	b := &BigQuery{}

	driver, err := b.Open(connectionUrl)
	if err != nil {
		return nil, err
	}

	return driver, nil
}

func TestWithInstanceWithoutClient(t *testing.T) {
	driver, err := WithInstance(context.Background(), nil, &Config{})
	if err == nil {
		t.Errorf("expected `no client`, got nil")
		return
	}
	if !errors.Is(err, ErrNoClient) {
		t.Errorf("expected `no client`, got %s", err.Error())
		return
	}
	if driver != nil {
		t.Errorf("driver should be nil")
		return
	}
}

func TestOpen(t *testing.T) {
	driver, err := openConnection()
	if err != nil {
		t.Error(err)
		return
	}

	defer func() {
		err := driver.Close()
		if err != nil {
			t.Error(err)
		}
	}()
}

func TestClose(t *testing.T) {
	driver, err := openConnection()
	if err != nil {
		t.Error(err)
		return
	}

	defer func() {
		err := driver.Close()
		if err != nil {
			t.Error(err)
		}
	}()

	err = driver.Close()
	if err != nil {
		t.Error(err)
		return
	}
}

func TestVersion(t *testing.T) {
	driver, err := openConnection()
	if err != nil {
		t.Error(err)
		return
	}

	defer func() {
		err := driver.Close()
		if err != nil {
			t.Error(err)
		}
	}()

	version, dirty, err := driver.Version()
	if err != nil {
		t.Error(err)
		return
	}

	t.Log(version, dirty)
}

func TestSetVersion(t *testing.T) {
	driver, err := openConnection()
	if err != nil {
		t.Error(err)
		return
	}

	defer func() {
		err := driver.Close()
		if err != nil {
			t.Error(err)
		}
	}()

	err = driver.SetVersion(-1, false)
	if err != nil {
		t.Error(err)
		return
	}
}

func TestDrop(t *testing.T) {
	driver, err := openConnection()
	if err != nil {
		t.Error(err)
		return
	}

	defer func() {
		err := driver.Close()
		if err != nil {
			t.Error(err)
		}
	}()

	err = driver.Drop()
	if err != nil {
		t.Error(err)
		return
	}
}

func TestRun(t *testing.T) {
	driver, err := openConnection()
	if err != nil {
		t.Error(err)
		return
	}

	defer func() {
		err := driver.Close()
		if err != nil {
			t.Error(err)
		}
	}()

	err = driver.Run(strings.NewReader(`
		CREATE TABLE IF NOT EXISTS users (
			first_name STRING,
		  	last_name STRING
		)`))
	if err != nil {
		t.Error(err)
		return
	}
}

func TestRunWithError(t *testing.T) {
	driver, err := openConnection()
	if err != nil {
		t.Error(err)
		return
	}

	defer func() {
		err := driver.Close()
		if err != nil {
			t.Error(err)
		}
	}()

	err = driver.Run(strings.NewReader(`
		CREATE TABLE IF NOT EXISTS users (
			first_name STRINGa,
		  	last_name STRING
		)`))
	if err == nil {
		t.Error("expected 'googleapi: Error 400: Query error: Type not found: STRINGa at [4:36], invalidQuery' got nil")
	}

	t.Log(err)
}
