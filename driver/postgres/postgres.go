package postgres

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"github.com/mattes/migrate/file"
	"github.com/mattes/migrate/migrate/direction"
)

type Driver struct {
	db *sql.DB
}

const tableName = "schema_migrations"

func (driver *Driver) Initialize(url string) error {
	db, err := sql.Open("postgres", url)
	if err != nil {
		return err
	}
	if err := db.Ping(); err != nil {
		return err
	}
	driver.db = db

	if err := driver.ensureVersionTableExists(); err != nil {
		return err
	}
	return nil
}

func (driver *Driver) ensureVersionTableExists() error {
	if _, err := driver.db.Exec(`CREATE TABLE IF NOT EXISTS ` + tableName + ` (
			version int not null primary key
    );`); err != nil {
		return err
	}
	return nil
}

func (driver *Driver) FilenameExtension() string {
	return "sql"
}

func (driver *Driver) Migrate(files file.Files, pipe chan interface{}) {
	defer close(pipe)

	for _, f := range files {

		direc := ""
		if f.Direction == direction.Up {
			direc = "  →"
		} else if f.Direction == direction.Down {
			direc = "←  "
		}
		pipe <- fmt.Sprintf("%s | %s", direc, f.FileName)

		tx, err := driver.db.Begin()
		if err != nil {
			pipe <- err
			return
		}

		if f.Direction == direction.Up {
			if _, err := tx.Exec(`INSERT INTO `+tableName+` (version) VALUES ($1)`, f.Version); err != nil {
				pipe <- err
				if err := tx.Rollback(); err != nil {
					pipe <- err
				}
				return
			}
		} else if f.Direction == direction.Down {
			if _, err := tx.Exec(`DELETE FROM `+tableName+` WHERE version=$1`, f.Version); err != nil {
				pipe <- err
				if err := tx.Rollback(); err != nil {
					pipe <- err
				}
				return
			}
		}

		f.Read()
		if _, err := tx.Exec(string(f.Content)); err != nil {
			pipe <- err
			if err := tx.Rollback(); err != nil {
				pipe <- err
			}
			return
		}

		if err := tx.Commit(); err != nil {
			pipe <- err
			return
		}
	}

	return
}

func (driver *Driver) Version() (uint64, error) {
	var version uint64
	err := driver.db.QueryRow(`SELECT version FROM ` + tableName + ` ORDER BY version DESC`).Scan(&version)
	switch {
	case err == sql.ErrNoRows:
		return 0, nil
	case err != nil:
		return 0, err
	default:
		return version, nil
	}
}
