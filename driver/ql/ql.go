package ql

import (
	"database/sql"

	"github.com/mattes/migrate/file"
	"github.com/mattes/migrate/driver"
	"github.com/mattes/migrate/migrate/direction"

	_ "github.com/cznic/ql/driver"
	"strings"
	"fmt"
)

const tableName = "schema_migration"

type Driver struct {
	db *sql.DB
}

func (d *Driver) Initialize(url string) (err error) {
	d.db, err = sql.Open("ql", strings.TrimPrefix(url, "ql+"))
	if err != nil {
		return
	}
	if err = d.db.Ping(); err != nil {
		return
	}
	if err = d.ensureVersionTableExists(); err != nil {
		return
	}
	return
}

func (d *Driver) Close() error {
	if err := d.db.Close(); err != nil {
		return err
	}
	return nil
}

func (d *Driver) FilenameExtension() string {
	return "sql"
}

func (d *Driver) Migrate(f file.File, pipe chan interface{}) {
	defer close(pipe)
	pipe <- f

	tx, err := d.db.Begin()
	if err != nil {
		pipe <- err
		return
	}

	switch f.Direction {
	case direction.Up:
		if _, err := tx.Exec("INSERT INTO "+tableName+" (version) VALUES (uint($1))", f.Version); err != nil {
			pipe <- err
			if err := tx.Rollback(); err != nil {
				pipe <- err
			}
			return
		}
	case direction.Down:
		if _, err := tx.Exec("DELETE FROM "+tableName+" WHERE version=uint($1)", f.Version); err != nil {
			pipe <- err
			if err := tx.Rollback(); err != nil {
				pipe <- err
			}
			return
		}
	}

	if err := f.ReadContent(); err != nil {
		pipe <- err
		return
	}

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

func (d *Driver) Version() (uint64, error) {
	var version uint64
	err := d.db.QueryRow("SELECT version FROM " + tableName + " ORDER BY version DESC LIMIT 1").Scan(&version)
	switch {
	case err == sql.ErrNoRows:
		return 0, nil
	case err != nil:
		return 0, err
	default:
		return version, nil
	}
}

func (d *Driver) ensureVersionTableExists() error {
	tx, err := d.db.Begin()
	if err != nil {
		return err
	}
	if _, err := tx.Exec(fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (version uint64);
		CREATE UNIQUE INDEX IF NOT EXISTS version_unique ON %s (version);
	`, tableName, tableName)); err != nil {
		if err := tx.Rollback(); err != nil {
			return err
		}
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

func init() {
	driver.RegisterDriver("ql+file", &Driver{})
	driver.RegisterDriver("ql+memory", &Driver{})
}