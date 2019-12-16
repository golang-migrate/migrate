package httpfs

import (
	"errors"
	"net/http"

	"github.com/golang-migrate/migrate/v4/source"
)

// driver is a migration source driver for reading migrations from
// http.FileSystem instances. It implements source.Driver interface and can be
// used as a migration source for the main migrate library.
type driver struct {
	Migrator
}

// New creates a new migrate source driver from a http.FileSystem instance and a
// relative path to migration files within the virtual FS. It will delay any
// errors until first usage of this driver.
func New(fs http.FileSystem, path string) source.Driver {
	var d driver
	if err := d.Init(fs, path); err != nil {
		return &failedDriver{err}
	}
	return &d
}

// Open completes the implementetion of source.Driver interface. Other methods
// are implemented by the embedded Migrator struct.
func (d *driver) Open(url string) (source.Driver, error) {
	return nil, errors.New("not implemented")
}
