package source

import (
	"fmt"
	"io"
	nurl "net/url"
	"sync"
)

var driversMu sync.RWMutex
var drivers = make(map[string]Driver)

type Driver interface {
	Open(url string) (Driver, error)

	Close() error

	First() (version uint, err error)

	Prev(version uint) (prevVersion uint, err error)

	Next(version uint) (nextVersion uint, err error)

	ReadUp(version uint) (r io.ReadCloser, identifier string, err error)

	ReadDown(version uint) (r io.ReadCloser, identifier string, err error)
}

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
