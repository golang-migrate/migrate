// Package postgres implements the Driver interface.
package postgres

import (
	"database/sql"
	"errors"
	"fmt"
	"strconv"

	"github.com/lib/pq"
	"github.com/mattes/migrate/driver"
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

func (driver *Driver) Close() error {
	if err := driver.db.Close(); err != nil {
		return err
	}
	return nil
}

func (driver *Driver) ensureVersionTableExists() error {
	r := driver.db.QueryRow("SELECT count(*) FROM information_schema.tables WHERE table_name = $1 AND table_schema = (SELECT current_schema());", tableName)
	c := 0
	if err := r.Scan(&c); err != nil {
		return err
	}
	if c > 0 {
		return nil
	}
	if _, err := driver.db.Exec("CREATE TABLE IF NOT EXISTS " + tableName + " (version bigint not null primary key);"); err != nil {
		return err
	}
	return nil
}

func (driver *Driver) FilenameExtension() string {
	return "sql"
}

func (driver *Driver) Migrate(f file.File, pipe chan interface{}) {
	defer close(pipe)
	pipe <- f

	tx, err := driver.db.Begin()
	if err != nil {
		pipe <- err
		return
	}

	if f.Direction == direction.Up {
		if _, err := tx.Exec("INSERT INTO "+tableName+" (version) VALUES ($1)", f.Version); err != nil {
			pipe <- err
			if err := tx.Rollback(); err != nil {
				pipe <- err
			}
			return
		}
	} else if f.Direction == direction.Down {
		if _, err := tx.Exec("DELETE FROM "+tableName+" WHERE version=$1", f.Version); err != nil {
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
		switch pqErr := err.(type) {
		case *pq.Error:
			offset, err := strconv.Atoi(pqErr.Position)
			if err == nil && offset >= 0 {
				lineNo, columnNo := file.LineColumnFromOffset(f.Content, offset-1)
				errorPart := file.LinesBeforeAndAfter(f.Content, lineNo, 5, 5, true)
				pipe <- errors.New(fmt.Sprintf("%s %v: %s in line %v, column %v:\n\n%s", pqErr.Severity, pqErr.Code, pqErr.Message, lineNo, columnNo, string(errorPart)))
			} else {
				pipe <- errors.New(fmt.Sprintf("%s %v: %s", pqErr.Severity, pqErr.Code, pqErr.Message))
			}

			if err := tx.Rollback(); err != nil {
				pipe <- err
			}
			return
		default:
			pipe <- err
			if err := tx.Rollback(); err != nil {
				pipe <- err
			}
			return
		}
	}

	if err := tx.Commit(); err != nil {
		pipe <- err
		return
	}
}

func (driver *Driver) Version() (uint64, error) {
	var version uint64
	err := driver.db.QueryRow("SELECT version FROM " + tableName + " ORDER BY version DESC LIMIT 1").Scan(&version)
	switch {
	case err == sql.ErrNoRows:
		return 0, nil
	case err != nil:
		return 0, err
	default:
		return version, nil
	}
}

func init() {
	driver.RegisterDriver("postgres", &Driver{})
}
