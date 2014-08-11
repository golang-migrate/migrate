package postgres

import (
	"database/sql"
	_ "github.com/lib/pq"
	"github.com/mattes/migrate/file"
	"github.com/mattes/migrate/migrate/direction"
	"testing"
)

func TestMigrate(t *testing.T) {
	connection, err := sql.Open("postgres", "postgres://localhost/migratetest?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}

	if _, err := connection.Exec(`DROP TABLE IF EXISTS hello; DROP TABLE IF EXISTS yolo; DROP TABLE IF EXISTS ` + tableName + `;`); err != nil {
		t.Fatal(err)
	}

	d := &Driver{}
	if err := d.Initialize("postgres://localhost/migratetest?sslmode=disable"); err != nil {
		t.Fatal(err)
	}

	version, err := d.Version()
	if err != nil {
		t.Fatal(err)
	}
	if version != 0 {
		t.Fatal("wrong version", version)
	}

	files := make(file.MigrationFiles, 0)
	files = append(files, file.MigrationFile{
		Version: 1,
		UpFile: &file.File{
			Path:      "/tmp",
			FileName:  "001_initial.up.sql",
			Version:   1,
			Name:      "initial",
			Direction: direction.Up,
			Content: []byte(`
				CREATE TABLE hello (
					id serial not null primary key,
					message varchar(255) not null default ''
				);

				CREATE TABLE yolo (
					id serial not null primary key,
					foobar varchar(255) not null default ''
				);
			`),
		},
		DownFile: &file.File{
			Path:      "/tmp",
			FileName:  "001_initial.down.sql",
			Version:   1,
			Name:      "initial",
			Direction: direction.Down,
			Content: []byte(`
				DROP TABLE IF EXISTS hello;
				DROP TABLE IF EXISTS yolo;
			`),
		},
	})

	applyFiles, _ := files.ToLastFrom(0)
	if err := d.Migrate(applyFiles); err != nil {
		t.Fatal(err)
	}

	version, _ = d.Version()
	if version != 1 {
		t.Fatalf("wrong version %v expected 1", version)
	}

	if _, err := connection.Exec(`INSERT INTO hello (message) VALUES ($1)`, "whats up"); err != nil {
		t.Fatal("Migrations failed")
	}

	applyFiles2, _ := files.ToFirstFrom(1)
	if err := d.Migrate(applyFiles2); err != nil {
		t.Fatal(err)
	}

	version, _ = d.Version()
	if version != 0 {
		t.Fatalf("wrong version %v expected 0", version)
	}

	if _, err := connection.Exec(`INSERT INTO hello (message) VALUES ($1)`, "whats up"); err == nil {
		t.Fatal("Migrations failed")
	}

}
