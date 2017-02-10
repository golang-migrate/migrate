// Package source provides the Source interface.
// All source drivers must implement this interface, register themselves,
// optionally provide a `WithInstance` function and pass the tests
// in package source/testing.
package source

import (
	"fmt"
	"io"
	nurl "net/url"
	"sync"
)

var driversMu sync.RWMutex
var drivers = make(map[string]Driver)

// Driver is an interface every driver must implement.
// The driver implementation must pass the `Test` in source/testing.
// Optionally provide a `WithInstance` function, so users can bypass `Open`
// and use an existing source instance.
type Driver interface {
	// Open returns a a new driver instance configured with parameters
	// coming from the URL string. Migrate will call this function
	// only once per instance.
	Open(url string) (Driver, error)

	// Close closes the underlying source instance managed by the driver.
	// Migrate will call this function only once per instance.
	Close() error

	// First returns the very first migration version available to the driver.
	// Migrate will call this function multiple times.
	// If there is no version available, it must return os.ErrNotExist.
	First() (version uint, err error)

	// Prev returns the previous version for a given version available to the driver.
	// Migrate will call this function multiple times.
	// If there is no previous version available, it must return os.ErrNotExist.
	Prev(version uint) (prevVersion uint, err error)

	// Next returns the next version for a given version available to the driver.
	// Migrate will call this function multiple times.
	// If there is no next version available, it must return os.ErrNotExist.
	Next(version uint) (nextVersion uint, err error)

	// ReadUp returns the UP migration body and an identifier that helps
	// finding this migration in the source for a given version.
	// If there is no up migration available for this version,
	// it must return os.ErrNotExist.
	// Do not start reading, just return the ReadCloser!
	ReadUp(version uint) (r io.ReadCloser, identifier string, err error)

	// ReadDown returns the DOWN migration body and an identifier that helps
	// finding this migration in the source for a given version.
	// If there is no down migration available for this version,
	// it must return os.ErrNotExist.
	// Do not start reading, just return the ReadCloser!
	ReadDown(version uint) (r io.ReadCloser, identifier string, err error)
}

// Open returns a new driver instance.
func Open(url string) (Driver, error) {
	u, err := nurl.Parse(url)
	if err != nil {
		return nil, err
	}

	if u.Scheme == "" {
		return nil, fmt.Errorf("source driver: invalid URL scheme")
	}

	driversMu.RLock()
	d, ok := drivers[u.Scheme]
	driversMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("source driver: unknown driver %v (forgotton import?)", u.Scheme)
	}

	return d.Open(url)
}

// Register globally registers a driver.
func Register(name string, driver Driver) {
	driversMu.Lock()
	defer driversMu.Unlock()
	if driver == nil {
		panic("Register driver is nil")
	}
	if _, dup := drivers[name]; dup {
		panic("Register called twice for driver " + name)
	}
	drivers[name] = driver
}
