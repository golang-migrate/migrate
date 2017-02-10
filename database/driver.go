// Package database provides the Database interface.
// All database drivers must implement this interface, register themselves,
// optionally provide a `WithInstance` function and pass the tests
// in package database/testing.
package database

import (
	"fmt"
	"io"
	nurl "net/url"
	"sync"
)

var (
	ErrLocked = fmt.Errorf("can't acquire lock")
)

const NilVersion int = -1

var driversMu sync.RWMutex
var drivers = make(map[string]Driver)

// Driver is an interface every driver must implement.
// The driver implementation must pass the `Test` in database/testing.
// Optionally provide a `WithInstance` function, so users can bypass `Open`
// and use an existing database instance.
//
// Implementations must not assume things nor try to correct user input.
// If in doubt, return an error.
type Driver interface {
	// Open returns a new driver instance configured with parameters
	// coming from the URL string. Migrate will call this function
	// only once per instance.
	Open(url string) (Driver, error)

	// Close closes the underlying database instance managed by the driver.
	// Migrate will call this function only once per instance.
	Close() error

	// Lock should acquire a database lock so that only one migration process
	// can run at a time. Migrate will call this function before Run is called.
	// If the implementation can't provide this functionality, return nil.
	Lock() error

	// Unlock should release the lock. Migrate will call this function after
	// all migrations have been run.
	Unlock() error

	// Run applies a migration to the database. Run the migration and store
	// the version. migration can be nil. In that case, just store the version.
	// When version -1 is given, the state should be as if no migration had been run.
	Run(version int, migration io.Reader) error

	// Version returns the currently active version.
	// When no migration has been run yet, it must return -1.
	// If the returned version is < -1 it will panic (in the test).
	Version() (int, error)

	// Drop deletes everyting in the database.
	Drop() error
}

// Open returns a new driver instance.
func Open(url string) (Driver, error) {
	u, err := nurl.Parse(url)
	if err != nil {
		return nil, err
	}

	if u.Scheme == "" {
		return nil, fmt.Errorf("database driver: invalid URL scheme")
	}

	driversMu.RLock()
	d, ok := drivers[u.Scheme]
	driversMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("database driver: unknown driver %v (forgotton import?)", u.Scheme)
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
