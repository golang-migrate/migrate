// Package cassandra implements the Driver interface.
package cassandra

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/gocql/gocql"
	"github.com/mattes/migrate/driver"
	"github.com/mattes/migrate/file"
	"github.com/mattes/migrate/migrate/direction"
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
// cassandra://host:port/keyspace?protocol=version&consistency=level
//
// Examples:
// cassandra://localhost/SpaceOfKeys?protocol=4
// cassandra://localhost/SpaceOfKeys?protocol=4&consistency=all
// cassandra://localhost/SpaceOfKeys?consistency=quorum
func (driver *Driver) Initialize(rawurl string) error {
	u, err := url.Parse(rawurl)

	cluster := gocql.NewCluster(u.Host)
	cluster.Keyspace = u.Path[1:len(u.Path)]
	cluster.Consistency = gocql.All
	cluster.Timeout = 1 * time.Minute

	if len(u.Query().Get("consistency")) > 0 {
		consistency, err := parseConsistency(u.Query().Get("consistency"))
		if err != nil {
			return err
		}

		cluster.Consistency = consistency
	}

	if len(u.Query().Get("protocol")) > 0 {
		protoversion, err := strconv.Atoi(u.Query().Get("protocol"))
		if err != nil {
			return err
		}

		cluster.ProtoVersion = protoversion
	}

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

func init() {
	driver.RegisterDriver("cassandra", &Driver{})
}

// ParseConsistency wraps gocql.ParseConsistency to return an error
// instead of a panicing.
func parseConsistency(consistencyStr string) (consistency gocql.Consistency, err error) {
	defer func() {
		if r := recover(); r != nil {
			var ok bool
			err, ok = r.(error)
			if !ok {
				err = fmt.Errorf("Failed to parse consistency \"%s\": %v", consistencyStr, r)
			}
		}
	}()
	consistency = gocql.ParseConsistency(consistencyStr)

	return consistency, nil
}
