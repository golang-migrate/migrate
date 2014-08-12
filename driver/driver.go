package driver

import (
	"errors"
	"fmt"
	"github.com/mattes/migrate/driver/bash"
	"github.com/mattes/migrate/driver/postgres"
	"github.com/mattes/migrate/file"
	neturl "net/url" // alias to allow `url string` func signature in New
)

type Driver interface {
	Initialize(url string) error
	FilenameExtension() string
	Migrate(files file.Files, pipe chan interface{})
	Version() (uint64, error)
}

// InitDriver returns Driver and initializes it
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
	case "bash":
		d := &bash.Driver{}
		verifyFilenameExtension("bash", d)
		if err := d.Initialize(url); err != nil {
			return nil, err
		}
		return d, nil
	default:
		return nil, errors.New(fmt.Sprintf("Driver '%s' not found.", u.Scheme))
	}
}

func verifyFilenameExtension(driverName string, d Driver) {
	f := d.FilenameExtension()
	if f == "" {
		panic(fmt.Sprintf("%s.FilenameExtension() returns empty string.", driverName))
	}
	if f[0:1] == "." {
		panic(fmt.Sprintf("%s.FilenameExtension() returned string must not start with a dot.", driverName))
	}
}
