package httpfs

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"path"

	"github.com/golang-migrate/migrate/v4/source"
)

// driver is a migration source driver for reading migrations from
// http.FileSystem instances. It implements source.Driver interface and can be
// used as a migration source for the main migrate library.
type driver struct {
	migrations *source.Migrations
	fs         http.FileSystem
	path       string
}

// New creates a new migrate source driver from a http.FileSystem instance and a
// relative path to migration files within the virtual FS. It is identical to
// the WithInstance() function except it will delay any errors on fist usage of
// this driver. This reduces the number of error handling branches in clients of
// this package without compromising on correctness.
func New(fs http.FileSystem, path string) source.Driver {
	d, err := newDriver(fs, path)
	if err != nil {
		return &failedDriver{err}
	}
	return d
}

// WithInstance creates new migrate source driver from a http.FileSystem
// instance and a relative path to migration files within the virtual FS.
func WithInstance(fs http.FileSystem, path string) (source.Driver, error) {
	return newDriver(fs, path)
}

func newDriver(fs http.FileSystem, path string) (*driver, error) {
	root, err := fs.Open(path)
	if err != nil {
		return nil, err
	}

	files, err := root.Readdir(0)
	if err != nil {
		_ = root.Close()
		return nil, err
	}
	if err = root.Close(); err != nil {
		return nil, err
	}

	ms := source.NewMigrations()
	for _, file := range files {
		if file.IsDir() {
			continue
		}

		m, err := source.DefaultParse(file.Name())
		if err != nil {
			continue // ignore files that we can't parse
		}

		if !ms.Append(m) {
			return nil, fmt.Errorf("duplicate migration file: %v", file.Name())
		}
	}

	return &driver{
		fs:         fs,
		path:       path,
		migrations: ms,
	}, nil
}

// Open is part of source.Driver interface implementation. It always returns
// error because http.FileSystem must be provided by the user of this package
// and created using WithInstance() function.
func (h *driver) Open(url string) (source.Driver, error) {
	return nil, fmt.Errorf("not implemented")
}

// Close is part of source.Driver interface implementation. This is a no-op.
func (h *driver) Close() error {
	return nil
}

// First is part of source.Driver interface implementation.
func (h *driver) First() (version uint, err error) {
	if version, ok := h.migrations.First(); ok {
		return version, nil
	}
	return 0, &os.PathError{
		Op:   "first",
		Path: h.path,
		Err:  os.ErrNotExist,
	}
}

// Prev is part of source.Driver interface implementation.
func (h *driver) Prev(version uint) (prevVersion uint, err error) {
	if version, ok := h.migrations.Prev(version); ok {
		return version, nil
	}
	return 0, &os.PathError{
		Op:   fmt.Sprintf("prev for version %v", version),
		Path: h.path,
		Err:  os.ErrNotExist,
	}
}

// Next is part of source.Driver interface implementation.
func (h *driver) Next(version uint) (nextVersion uint, err error) {
	if version, ok := h.migrations.Next(version); ok {
		return version, nil
	}
	return 0, &os.PathError{
		Op:   fmt.Sprintf("next for version %v", version),
		Path: h.path,
		Err:  os.ErrNotExist,
	}
}

// ReadUp is part of source.Driver interface implementation.
func (h *driver) ReadUp(version uint) (r io.ReadCloser, identifier string, err error) {
	if m, ok := h.migrations.Up(version); ok {
		body, err := h.fs.Open(path.Join(h.path, m.Raw))
		if err != nil {
			return nil, "", err
		}
		return body, m.Identifier, nil
	}
	return nil, "", &os.PathError{
		Op:   fmt.Sprintf("read version %v", version),
		Path: h.path,
		Err:  os.ErrNotExist,
	}
}

// ReadDown is part of source.Driver interface implementation.
func (h *driver) ReadDown(version uint) (r io.ReadCloser, identifier string, err error) {
	if m, ok := h.migrations.Down(version); ok {
		body, err := h.fs.Open(path.Join(h.path, m.Raw))
		if err != nil {
			return nil, "", err
		}
		return body, m.Identifier, nil
	}
	return nil, "", &os.PathError{
		Op:   fmt.Sprintf("read version %v", version),
		Path: h.path,
		Err:  os.ErrNotExist,
	}
}
