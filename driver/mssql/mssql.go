// Package mssql implements the Driver interface.
package mssql

import (
	"database/sql"
	"errors"
	"fmt"

	"github.com/denisenkom/go-mssqldb"
	"github.com/mattes/migrate/driver"
	"github.com/mattes/migrate/file"
	"github.com/mattes/migrate/migrate/direction"
)

type Driver struct {
	db *sql.DB
}

const tableName = "schema_migrations"

func (driver *Driver) Initialize(url string) error {
	db, err := sql.Open("mssql", url)
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
	if _, err := driver.db.Exec("if object_id('" + tableName + "') is null create table [" + tableName + "] ([version] BIGINT NOT NULL PRIMARY KEY);"); err != nil {
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
		if _, err := tx.Exec("INSERT INTO "+tableName+" (version) VALUES (?)", f.Version); err != nil {
			pipe <- err
			if err := tx.Rollback(); err != nil {
				pipe <- err
			}
			return
		}
	} else if f.Direction == direction.Down {
		if _, err := tx.Exec("DELETE FROM "+tableName+" WHERE version=?", f.Version); err != nil {
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
		switch msErr := err.(type) {
		case *mssql.Error:
			errorPart := file.LinesBeforeAndAfter(f.Content, int(msErr.LineNo), 5, 5, true)
			pipe <- errors.New(fmt.Sprintf("%v: %s in line %v:\n\n%s", msErr.Number, msErr.Message, msErr.LineNo, string(errorPart)))
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
	err := driver.db.QueryRow("SELECT version FROM " + tableName + " ORDER BY version DESC OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY").Scan(&version)
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
	driver.RegisterDriver("mssql", &Driver{})
	driver.RegisterDriver("sqlserver", &Driver{})
}
