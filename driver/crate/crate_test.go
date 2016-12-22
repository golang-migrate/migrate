package crate

import (
	"fmt"
	"os"
	"testing"

	"github.com/mattes/migrate/file"
	"github.com/mattes/migrate/migrate/direction"
	pipep "github.com/mattes/migrate/pipe"
)

func TestContentSplit(t *testing.T) {
	content := `CREATE TABLE users (user_id STRING primary key, first_name STRING, last_name STRING, email STRING, password_hash STRING) CLUSTERED INTO 3 shards WITH (number_of_replicas = 0);
CREATE TABLE units (unit_id STRING primary key, name STRING, members array(string)) CLUSTERED INTO 3 shards WITH (number_of_replicas = 0);
CREATE TABLE available_connectors (technology_id STRING primary key, description STRING, icon STRING, link STRING, configuration_parameters array(object as (name STRING, type STRING))) CLUSTERED INTO 3 shards WITH (number_of_replicas = 0);
	`

	lines := splitContent(content)
	if len(lines) != 3 {
		t.Errorf("Expected 3 lines, but got %d", len(lines))
	}

	if lines[0] != "CREATE TABLE users (user_id STRING primary key, first_name STRING, last_name STRING, email STRING, password_hash STRING) CLUSTERED INTO 3 shards WITH (number_of_replicas = 0)" {
		t.Error("Line does not match expected output")
	}

	if lines[1] != "CREATE TABLE units (unit_id STRING primary key, name STRING, members array(string)) CLUSTERED INTO 3 shards WITH (number_of_replicas = 0)" {
		t.Error("Line does not match expected output")
	}

	if lines[2] != "CREATE TABLE available_connectors (technology_id STRING primary key, description STRING, icon STRING, link STRING, configuration_parameters array(object as (name STRING, type STRING))) CLUSTERED INTO 3 shards WITH (number_of_replicas = 0)" {
		t.Error("Line does not match expected output")
	}
}

func TestMigrate(t *testing.T) {
	host := os.Getenv("CRATE_PORT_4200_TCP_ADDR")
	port := os.Getenv("CRATE_PORT_4200_TCP_PORT")

	url := fmt.Sprintf("crate://%s:%s", host, port)

	driver := &Driver{}

	if err := driver.Initialize(url); err != nil {
		t.Fatal(err)
	}

	successFiles := []file.File{
		{
			Path:      "/foobar",
			FileName:  "001_foobar.up.sql",
			Version:   1,
			Name:      "foobar",
			Direction: direction.Up,
			Content: []byte(`
                CREATE TABLE yolo (
                    id integer primary key,
                    msg string
                );
            `),
		},
		{
			Path:      "/foobar",
			FileName:  "002_foobar.down.sql",
			Version:   1,
			Name:      "foobar",
			Direction: direction.Down,
			Content: []byte(`
                DROP TABLE yolo;
            `),
		},
	}

	failFiles := []file.File{
		{
			Path:      "/foobar",
			FileName:  "002_foobar.up.sql",
			Version:   2,
			Name:      "foobar",
			Direction: direction.Up,
			Content: []byte(`
                CREATE TABLE error (
                    id THIS WILL CAUSE AN ERROR
                )
            `),
		},
	}

	for _, file := range successFiles {
		pipe := pipep.New()
		go driver.Migrate(file, pipe)
		errs := pipep.ReadErrors(pipe)
		if len(errs) > 0 {
			t.Fatal(errs)
		}
	}

	for _, file := range failFiles {
		pipe := pipep.New()
		go driver.Migrate(file, pipe)
		errs := pipep.ReadErrors(pipe)
		if len(errs) == 0 {
			t.Fatal("Migration should have failed but succeeded")
		}
	}

	if err := driver.Close(); err != nil {
		t.Fatal(err)
	}
}
