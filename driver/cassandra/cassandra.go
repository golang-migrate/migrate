// Package cassandra implements the Driver interface.
package cassandra

import (
	"fmt"
	"github.com/gocql/gocql"
	"github.com/mattes/migrate/file"
	"github.com/mattes/migrate/migrate/direction"
	"net/url"
	"strings"
	"time"
)

type Driver struct {
	session *gocql.Session
}

const (
	tableName  = "schema_migrations"
	versionRow = 1
)

type counterStmt bool

func (c counterStmt) String() string {
	sign := ""
	if bool(c) {
		sign = "+"
	} else {
		sign = "-"
	}
	return "UPDATE " + tableName + " SET version = version " + sign + " 1 where versionRow = ?"
}

const (
	up   counterStmt = true
	down counterStmt = false
)

// Cassandra Driver URL format:
// cassandra://host:port/keyspace
//
// Example:
// cassandra://localhost/SpaceOfKeys
func (driver *Driver) Initialize(rawurl string) error {
	u, err := url.Parse(rawurl)

	cluster := gocql.NewCluster(u.Host)
	cluster.Keyspace = u.Path[1:len(u.Path)]
	cluster.Consistency = gocql.All
	cluster.Timeout = 1 * time.Minute

	// Check if url user struct is null
	if u.User != nil {
		password, passwordSet := u.User.Password()

		if passwordSet == false {
			return fmt.Errorf("Missing password. Please provide password.")
		}

		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: u.User.Username(),
			Password: password,
		}

	}

	driver.session, err = cluster.CreateSession()

	if err != nil {
		return err
	}

	if err := driver.ensureVersionTableExists(); err != nil {
		return err
	}
	return nil
}

func (driver *Driver) Close() error {
	driver.session.Close()
	return nil
}

func (driver *Driver) ensureVersionTableExists() error {
	err := driver.session.Query("CREATE TABLE IF NOT EXISTS " + tableName + " (version counter, versionRow bigint primary key);").Exec()
	if err != nil {
		return err
	}

	_, err = driver.Version()
	if err != nil {
		driver.session.Query(up.String(), versionRow).Exec()
	}

	return nil
}

func (driver *Driver) FilenameExtension() string {
	return "cql"
}

func (driver *Driver) version(d direction.Direction, invert bool) error {
	var stmt counterStmt
	switch d {
	case direction.Up:
		stmt = up
	case direction.Down:
		stmt = down
	}
	if invert {
		stmt = !stmt
	}
	return driver.session.Query(stmt.String(), versionRow).Exec()
}

func (driver *Driver) Migrate(f file.File, pipe chan interface{}) {
	var err error
	defer func() {
		if err != nil {
			// Invert version direction if we couldn't apply the changes for some reason.
			if err := driver.version(f.Direction, true); err != nil {
				pipe <- err
			}
			pipe <- err
		}
		close(pipe)
	}()

	pipe <- f
	if err = driver.version(f.Direction, false); err != nil {
		return
	}

	if err = f.ReadContent(); err != nil {
		return
	}

	for _, query := range strings.Split(string(f.Content), ";") {
		query = strings.TrimSpace(query)
		if len(query) == 0 {
			continue
		}

		if err = driver.session.Query(query).Exec(); err != nil {
			return
		}
	}
}

func (driver *Driver) Version() (uint64, error) {
	var version int64
	err := driver.session.Query("SELECT version FROM "+tableName+" WHERE versionRow = ?", versionRow).Scan(&version)
	return uint64(version) - 1, err
}
