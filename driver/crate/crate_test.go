package crate

import (
	"fmt"
	"os"
	"testing"

	"github.com/mattes/migrate/file"
	"github.com/mattes/migrate/migrate/direction"
	pipep "github.com/mattes/migrate/pipe"
)

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
