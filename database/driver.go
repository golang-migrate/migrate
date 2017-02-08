package database

import (
	"fmt"
	"io"
	nurl "net/url"
	"sync"
)

var (
	ErrLocked = fmt.Errorf("unable to acquire lock")
)

const NilVersion int = -1

var driversMu sync.RWMutex
var drivers = make(map[string]Driver)

type Driver interface {
	Open(url string) (Driver, error)

	Close() error

	Lock() error

	Unlock() error

	// when version = NilVersion, "deinitialize"
	// migration can be nil, in that case, just store version
	Run(version int, migration io.Reader) error

	// version  > 0: regular version
	// version   -1: nil version (const NilVersion)
	// version < -1: will panic
	Version() (int, error)

	Drop() error
}

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
