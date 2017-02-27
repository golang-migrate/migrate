package postgres

import (
	"database/sql"
	"os"
	"testing"

	"github.com/mattes/migrate/file"
	"github.com/mattes/migrate/migrate/direction"
	pipep "github.com/mattes/migrate/pipe"
)

// TestMigrate runs some additional tests on Migrate().
// Basic testing is already done in migrate/migrate_test.go
func TestMigrate(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	host := os.Getenv("POSTGRES_PORT_5432_TCP_ADDR")
	port := os.Getenv("POSTGRES_PORT_5432_TCP_PORT")
	driverURL := "postgres://postgres@" + host + ":" + port + "/template1?sslmode=disable"

	// prepare clean database
	connection, err := sql.Open("postgres", driverURL)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := connection.Exec(`
				DROP TABLE IF EXISTS yolo;
				DROP TABLE IF EXISTS ` + tableName + `;`); err != nil {
		t.Fatal(err)
	}

	d := &Driver{}
	if err := d.Initialize(driverURL); err != nil {
		t.Fatal(err)
	}

	// testing idempotency: second call should be a no-op, since table already exists
	if err := d.Initialize(driverURL); err != nil {
		t.Fatal(err)
	}

	files := []file.File{
		{
			Path:      "/foobar",
			FileName:  "001_foobar.up.sql",
			Version:   1,
			Name:      "foobar",
			Direction: direction.Up,
			Content: []byte(`
				CREATE TABLE yolo (
					id serial not null primary key
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
		{
			Path:      "/foobar",
			FileName:  "20170118205923_demo.up.sql",
			Version:   20170118205923,
			Name:      "demo",
			Direction: direction.Up,
			Content: []byte(`
				CREATE TABLE demo (
					id serial not null primary key
				)
			`),
		},
		{
			Path:      "/foobar",
			FileName:  "20170118205923_demo.down.sql",
			Version:   20170118205923,
			Name:      "demo",
			Direction: direction.Down,
			Content: []byte(`
				DROP TABLE demo
			`),
		},
	}

	pipe := pipep.New()
	go d.Migrate(files[0], pipe)
	errs := pipep.ReadErrors(pipe)
	if len(errs) > 0 {
		t.Fatal(errs)
	}

	pipe = pipep.New()
	go d.Migrate(files[1], pipe)
	errs = pipep.ReadErrors(pipe)
	if len(errs) > 0 {
		t.Fatal(errs)
	}

	pipe = pipep.New()
	go d.Migrate(files[2], pipe)
	errs = pipep.ReadErrors(pipe)
	if len(errs) == 0 {
		t.Error("Expected test case to fail")
	}

	pipe = pipep.New()
	go d.Migrate(files[3], pipe)
	errs = pipep.ReadErrors(pipe)
	if len(errs) > 0 {
		t.Fatal(errs)
	}

	pipe = pipep.New()
	go d.Migrate(files[4], pipe)
	errs = pipep.ReadErrors(pipe)
	if len(errs) > 0 {
		t.Fatal(errs)
	}

	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
}
