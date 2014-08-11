package postgres

import (
	"database/sql"
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

func (driver *Driver) Migrate(files file.Files) error {
	for _, f := range files {

		tx, err := driver.db.Begin()
		if err != nil {
			return err
		}

		if f.Direction == direction.Up {
			if _, err := tx.Exec(`INSERT INTO `+tableName+` (version) VALUES ($1)`, f.Version); err != nil {
				if err := tx.Rollback(); err != nil {
					// haha, what now?
				}
				return err
			}
		} else if f.Direction == direction.Down {
			if _, err := tx.Exec(`DELETE FROM `+tableName+` WHERE version=$1`, f.Version); err != nil {
				if err := tx.Rollback(); err != nil {
					// haha, what now?
				}
				return err
			}
		}

		f.Read()
		if _, err := tx.Exec(string(f.Content)); err != nil {
			if err := tx.Rollback(); err != nil {
				// haha, what now?
			}
			return err
		}

		if err := tx.Commit(); err != nil {
			return err
		}
	}

	return nil
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
