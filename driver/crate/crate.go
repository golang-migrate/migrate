// Package crate implements a driver for the Crate.io database
package crate

import (
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/herenow/go-crate"
	"github.com/mattes/migrate/driver"
	"github.com/mattes/migrate/file"
	"github.com/mattes/migrate/migrate/direction"
)

func init() {
	driver.RegisterDriver("crate", &Driver{})
}

type Driver struct {
	db *sql.DB
}

const tableName = "schema_migrations"

func (driver *Driver) Initialize(url string) error {
	url = strings.Replace(url, "crate", "http", 1)
	db, err := sql.Open("crate", url)
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

func (driver *Driver) FilenameExtension() string {
	return "sql"
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

func (driver *Driver) Migrate(f file.File, pipe chan interface{}) {
	defer close(pipe)
	pipe <- f

	if err := f.ReadContent(); err != nil {
		pipe <- err
		return
	}

	lines := splitContent(string(f.Content))
	for _, line := range lines {
		_, err := driver.db.Exec(line)
		if err != nil {
			pipe <- err
			return
		}
	}

	if f.Direction == direction.Up {
		if _, err := driver.db.Exec("INSERT INTO "+tableName+" (version) VALUES (?)", f.Version); err != nil {
			pipe <- err
			return
		}
	} else if f.Direction == direction.Down {
		if _, err := driver.db.Exec("DELETE FROM "+tableName+" WHERE version=?", f.Version); err != nil {
			pipe <- err
			return
		}
	}
}

func splitContent(content string) []string {
	lines := strings.Split(content, ";")
	resultLines := make([]string, 0, len(lines))
	for i := range lines {
		line := strings.Replace(lines[i], ";", "", -1)
		line = strings.TrimSpace(line)
		if line != "" {
			resultLines = append(resultLines, line)
		}
	}
	return resultLines
}

func (driver *Driver) ensureVersionTableExists() error {
	if _, err := driver.db.Exec(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (version INTEGER PRIMARY KEY)", tableName)); err != nil {
		return err
	}
	return nil
}
