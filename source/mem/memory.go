package mem

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"sync"

	"github.com/golang-migrate/migrate/v4/source"
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
	currKey        string
	migrationsData *source.Migrations
}

// Ensures that Memory implements source.Driver
var _ source.Driver = (*Memory)(nil)

// WithInstance creates new instance using Memory driver.
func WithInstance(mig ...Migration) (source.Driver, error) {
	mem := &Memory{
		currKey:        "WithInstance",
		migrationsData: source.NewMigrations(),
	}

	mem.Lock()
	defer mem.Unlock()

	for _, m := range mig {
		if m == nil {
			return nil, ErrNilMigration
		}

		mem.migrationsData.Append(&source.Migration{
			Version:    m.Version(),
			Identifier: m.Up(),
			Direction:  source.Up,
			Raw:        fmt.Sprintf("%d.instance_memory.up", m.Version()),
		})

		mem.migrationsData.Append(&source.Migration{
			Version:    m.Version(),
			Identifier: m.Down(),
			Direction:  source.Down,
			Raw:        fmt.Sprintf("%d.instance_memory.down", m.Version()),
		})
	}

	return mem, nil
}

// Open create new source.Driver using Memory based on url string.
// url string should contain format: `inmem://{key}`
// where `{key}` is a unique identifier registered using RegisterMigrations method.
// ErrEmptyKey returned when key is not found, or other error will be thrown if unexpected things happen.
// This is part of source.Driver interface implementation.
func (m *Memory) Open(url string) (source.Driver, error) {
	url = strings.TrimSpace(url)
	if url == "" {
		return nil, ErrEmptyUrl
	}

	if !strings.HasPrefix(url, scheme) {
		return nil, ErrInvalidUrlScheme
	}

	key := strings.TrimPrefix(url, scheme)
	key = strings.TrimSpace(key)
	if key == "" {
		return nil, ErrEmptyKey
	}

	migrations.RLock()
	defer migrations.RUnlock()
	srcMigration, exist := migrations.data[key]
	if !exist {
		return nil, ErrNilMigration
	}

	return &Memory{
		currKey:        key,
		migrationsData: srcMigration,
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
	if v, ok := m.migrationsData.First(); ok {
		return v, nil
	}

	err = &os.PathError{
		Op:   "first",
		Path: fmt.Sprintf("%s%s", scheme, m.currKey),
		Err:  os.ErrNotExist,
	}
	return
}

// Prev return previous version of current version value.
// This is part of source.Driver interface implementation.
func (m *Memory) Prev(version uint) (prevVersion uint, err error) {
	if v, ok := m.migrationsData.Prev(version); ok {
		return v, nil
	}

	err = &os.PathError{
		Op:   fmt.Sprintf("prev for version %v", version),
		Path: fmt.Sprintf("%s%s", scheme, m.currKey),
		Err:  os.ErrNotExist,
	}
	return
}

// Next return next version of current version value.
// This is part of source.Driver interface implementation.
func (m *Memory) Next(version uint) (nextVersion uint, err error) {
	if v, ok := m.migrationsData.Next(version); ok {
		return v, nil
	}

	err = &os.PathError{
		Op:   fmt.Sprintf("next for version %v", version),
		Path: fmt.Sprintf("%s%s", scheme, m.currKey),
		Err:  os.ErrNotExist,
	}
	return
}

// ReadUp return the query that needs to be executed on source.Up command.
// This is part of source.Driver interface implementation.
func (m *Memory) ReadUp(version uint) (r io.ReadCloser, identifier string, err error) {
	migration, ok := m.migrationsData.Up(version)
	if !ok {
		err = &os.PathError{
			Op:   fmt.Sprintf("read up version %v", version),
			Path: fmt.Sprintf("%s%s", scheme, m.currKey),
			Err:  os.ErrNotExist,
		}
		return
	}

	r = ioutil.NopCloser(bytes.NewBufferString(migration.Identifier))
	identifier = fmt.Sprintf("%d.memory.up", version)
	return
}

// ReadDown return the query that needs to be executed on source.Down command.
// This is part of source.Driver interface implementation.
func (m *Memory) ReadDown(version uint) (r io.ReadCloser, identifier string, err error) {
	migration, ok := m.migrationsData.Down(version)
	if !ok {
		err = &os.PathError{
			Op:   fmt.Sprintf("read down version %v", version),
			Path: fmt.Sprintf("%s%s", scheme, m.currKey),
			Err:  os.ErrNotExist,
		}
		return
	}

	r = ioutil.NopCloser(bytes.NewBufferString(migration.Identifier))
	identifier = fmt.Sprintf("%d.memory.down", version)
	return
}
