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

// Driver implements migrate Driver interface
type Driver struct {
	session *gocql.Session
}

const (
	tableName = "schema_migrations"
)

// Initialize will be called first
func (driver *Driver) Initialize(rawurl string) error {
	u, err := url.Parse(rawurl)
	if err != nil {
		return fmt.Errorf("failed to parse connectil url: %v", err)
	}

	if u.Path == "" {
		return fmt.Errorf("no keyspace provided in connection url")
	}

	cluster := gocql.NewCluster(u.Host)
	cluster.Keyspace = u.Path[1:len(u.Path)]
	cluster.Consistency = gocql.All
	cluster.Timeout = 1 * time.Minute

	if len(u.Query().Get("consistency")) > 0 {
		var consistency gocql.Consistency
		consistency, err = parseConsistency(u.Query().Get("consistency"))
		if err != nil {
			return err
		}

		cluster.Consistency = consistency
	}

	if len(u.Query().Get("protocol")) > 0 {
		var protoversion int
		protoversion, err = strconv.Atoi(u.Query().Get("protocol"))
		if err != nil {
			return err
		}

		cluster.ProtoVersion = protoversion
	}

	// Check if url user struct is null
	if u.User != nil {
		password, passwordSet := u.User.Password()

		if passwordSet == false {
			return fmt.Errorf("Missing password. Please provide password")
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

// Close last function to be called. Closes cassandra session
func (driver *Driver) Close() error {
	driver.session.Close()
	return nil
}

func (driver *Driver) ensureVersionTableExists() error {
	err := driver.session.Query(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (id uuid primary key, version bigint)", tableName)).Exec()
	if err != nil {
		return err
	}

	if _, err = driver.Version(); err != nil {
		return err
	}

	return nil
}

// FilenameExtension return extension of migrations files
func (driver *Driver) FilenameExtension() string {
	return "cql"
}

func (driver *Driver) updateVersion(version uint64, dir direction.Direction) error {
	var ids []string
	var id string
	var err error
	iter := driver.session.Query(fmt.Sprintf("SELECT id FROM %s WHERE version >= ? ALLOW FILTERING", tableName), version).Iter()
	for iter.Scan(&id) {
		ids = append(ids, id)
	}
	if len(ids) > 0 {
		err = driver.session.Query(fmt.Sprintf("DELETE FROM %s WHERE id IN ?", tableName), ids).Exec()
		if err != nil {
			return err
		}
	}
	if dir == direction.Up {
		return driver.session.Query(fmt.Sprintf("INSERT INTO %s (id, version) VALUES (uuid(), ?)", tableName), version).Exec()
	}
	return nil
}

// Migrate run migration file. Restore previous version in case of fail
func (driver *Driver) Migrate(f file.File, pipe chan interface{}) {
	var err error
	previousVersion, err := driver.Version()
	if err != nil {
		close(pipe)
		return
	}
	defer func() {
		if err != nil {
			// Invert version direction if we couldn't apply the changes for some reason.
			if updErr := driver.updateVersion(previousVersion, direction.Up); updErr != nil {
				pipe <- updErr
			}
			pipe <- err
		}
		close(pipe)
	}()

	pipe <- f
	if err = driver.updateVersion(f.Version, f.Direction); err != nil {
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

// Version return current version
func (driver *Driver) Version() (uint64, error) {
	var version int64
	err := driver.session.Query(fmt.Sprintf("SELECT max(version) FROM %s", tableName)).Scan(&version)
	return uint64(version), err
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
