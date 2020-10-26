package mem

import (
	"fmt"
	"io"
	nurl "net/url"
	"strings"
	"sync"

	"github.com/golang-migrate/migrate/v4/source"
	"github.com/golang-migrate/migrate/v4/source/stub"
)

// init register to the source.Driver so it can be used easily
func init() {
	source.Register(schemeKey, &Memory{})
}

// Migration is an interface that holds migration info
type Migration interface {
	Version() uint
	Up() string
	Down() string
}

// Memory implements source.Driver interface and hold in memory migration
type Memory struct {
	sync.RWMutex
	stub *stub.Stub
}

// Ensures that Memory implements source.Driver
var _ source.Driver = (*Memory)(nil)

// WithInstance creates new instance using Memory driver.
func WithInstance(mig ...Migration) (source.Driver, error) {
	st := &stub.Stub{
		Url:        "",
		Instance:   nil,
		Migrations: source.NewMigrations(),
		Config:     &stub.Config{},
	}

	mem := &Memory{
		stub: st,
	}

	mem.Lock()
	defer mem.Unlock()

	for _, m := range mig {
		if m == nil {
			return nil, ErrNilMigration
		}

		mem.stub.Migrations.Append(&source.Migration{
			Version:    m.Version(),
			Identifier: m.Up(),
			Direction:  source.Up,
			Raw:        fmt.Sprintf("%d.instance_memory.up", m.Version()),
		})

		mem.stub.Migrations.Append(&source.Migration{
			Version:    m.Version(),
			Identifier: m.Down(),
			Direction:  source.Down,
			Raw:        fmt.Sprintf("%d.instance_memory.down", m.Version()),
		})
	}

	return mem, nil
}

// Open create new source.Driver using Memory based on url string.
// url string should contain format: `mem://{key}`
// where `{key}` is a unique identifier registered using RegisterMigrations method.
// ErrEmptyKey returned when key is not found, or other error will be thrown if unexpected things happen.
// This is part of source.Driver interface implementation.
func (m *Memory) Open(url string) (source.Driver, error) {
	u, err := nurl.Parse(url)
	if err != nil {
		return nil, err
	}

	if !strings.HasPrefix(u.Scheme, schemeKey) {
		return nil, ErrInvalidUrlScheme
	}

	key := strings.TrimSpace(u.Host)

	migrations.RLock()
	defer migrations.RUnlock()
	srcMigration, exist := migrations.data[key]
	if !exist {
		return nil, ErrNilMigration
	}

	st := &stub.Stub{
		Url:        "",
		Instance:   nil,
		Migrations: srcMigration,
		Config:     &stub.Config{},
	}

	return &Memory{
		stub: st,
	}, nil
}

// Close always return nil since there is nothing to close.
// This is part of source.Driver interface implementation.
func (m *Memory) Close() error {
	return nil
}

// First returns first migration data after sorted by version in ascending order.
// This is part of source.Driver interface implementation.
func (m *Memory) First() (version uint, err error) {
	return m.stub.First()
}

// Prev return previous version of current version value.
// This is part of source.Driver interface implementation.
func (m *Memory) Prev(version uint) (prevVersion uint, err error) {
	return m.stub.Prev(version)
}

// Next return next version of current version value.
// This is part of source.Driver interface implementation.
func (m *Memory) Next(version uint) (nextVersion uint, err error) {
	return m.stub.Next(version)
}

// ReadUp return the query that needs to be executed on source.Up command.
// This is part of source.Driver interface implementation.
func (m *Memory) ReadUp(version uint) (r io.ReadCloser, identifier string, err error) {
	return m.stub.ReadUp(version)
}

// ReadDown return the query that needs to be executed on source.Down command.
// This is part of source.Driver interface implementation.
func (m *Memory) ReadDown(version uint) (r io.ReadCloser, identifier string, err error) {
	return m.stub.ReadDown(version)
}
