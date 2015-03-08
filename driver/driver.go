// Package driver holds the driver interface.
package driver

import (
	"errors"
	"fmt"
	neturl "net/url" // alias to allow `url string` func signature in New

	"github.com/mattes/migrate/driver/bash"
	"github.com/mattes/migrate/driver/cassandra"
	"github.com/mattes/migrate/driver/mysql"
	"github.com/mattes/migrate/driver/postgres"
	"github.com/mattes/migrate/driver/sqlite3"
	"github.com/mattes/migrate/file"
)

// Driver is the interface type that needs to implemented by all drivers.
type Driver interface {

	// Initialize is the first function to be called.
	// Check the url string and open and verify any connection
	// that has to be made.
	Initialize(url string) error

	// Close is the last function to be called.
	// Close any open connection here.
	Close() error

	// FilenameExtension returns the extension of the migration files.
	// The returned string must not begin with a dot.
	FilenameExtension() string

	// Migrate is the heart of the driver.
	// It will receive a file which the driver should apply
	// to its backend or whatever. The migration function should use
	// the pipe channel to return any errors or other useful information.
	Migrate(file file.File, pipe chan interface{})

	// Version returns the current migration version.
	Version() (uint64, error)
}

// New returns Driver and calls Initialize on it
func New(url string) (Driver, error) {
	u, err := neturl.Parse(url)
	if err != nil {
		return nil, err
	}

	switch u.Scheme {
	case "postgres":
		d := &postgres.Driver{}
		verifyFilenameExtension("postgres", d)
		if err := d.Initialize(url); err != nil {
			return nil, err
		}
		return d, nil

	case "mysql":
		d := &mysql.Driver{}
		verifyFilenameExtension("mysql", d)
		if err := d.Initialize(url); err != nil {
			return nil, err
		}
		return d, nil

	case "bash":
		d := &bash.Driver{}
		verifyFilenameExtension("bash", d)
		if err := d.Initialize(url); err != nil {
			return nil, err
		}
		return d, nil

	case "cassandra":
		d := &cassandra.Driver{}
		verifyFilenameExtension("cassanda", d)
		if err := d.Initialize(url); err != nil {
			return nil, err
		}
		return d, nil
	case "sqlite3":
		d := &sqlite3.Driver{}
		verifyFilenameExtension("sqlite3", d)
		if err := d.Initialize(url); err != nil {
			return nil, err
		}
		return d, nil
	default:
		return nil, errors.New(fmt.Sprintf("Driver '%s' not found.", u.Scheme))
	}
}

// verifyFilenameExtension panics if the drivers filename extension
// is not correct or empty.
func verifyFilenameExtension(driverName string, d Driver) {
	f := d.FilenameExtension()
	if f == "" {
		panic(fmt.Sprintf("%s.FilenameExtension() returns empty string.", driverName))
	}
	if f[0:1] == "." {
		panic(fmt.Sprintf("%s.FilenameExtension() returned string must not start with a dot.", driverName))
	}
}
