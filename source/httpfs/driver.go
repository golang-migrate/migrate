package httpfs

import (
	"errors"
	"io"
	"net/http"
	"os"
	"path"
	"strconv"

	"github.com/golang-migrate/migrate/v4/source"
)

// driver is a migration source driver for reading migrations from
// http.FileSystem instances. It implements source.Driver interface and can be
// used as a migration source for the main migrate library.
type driver struct {
	Migrator
}

// New creates a new migrate source driver from a http.FileSystem instance and a
// relative path to migration files within the virtual FS. It is identical to
// the WithInstance() function except it will delay any errors on fist usage of
// this driver. This reduces the number of error handling branches in clients of
// this package without compromising on correctness.
func New(fs http.FileSystem, path string) source.Driver {
	var d driver
	if err := d.Init(fs, path); err != nil {
		return &failedDriver{err}
	}
	return &d
}

// Open is part of source.Driver interface implementation. It always returns
// error because http.FileSystem must be provided by the user of this package
// and created using WithInstance() function.
func (d *driver) Open(url string) (source.Driver, error) {
	return nil, errors.New("not implemented")
}

// Migrator is a helper service for creating new source drivers working with
// http.FileSystem instances. It implements all source.Driver interface methods
// except for Open(). New driver could embed this struct and add missing Open()
// method.
//
// To prepare Migrator for use Init() function.
type Migrator struct {
	migrations *source.Migrations
	fs         http.FileSystem
	path       string
}

// Init prepares not initialized Migrator instance to read migrations from a
// http.FileSystem instance and a relative path.
func (h *Migrator) Init(fs http.FileSystem, path string) error {
	root, err := fs.Open(path)
	if err != nil {
		return err
	}

	files, err := root.Readdir(0)
	if err != nil {
		_ = root.Close()
		return err
	}
	if err = root.Close(); err != nil {
		return err
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
			return source.ErrDuplicateMigration{
				Filename: file.Name(),
			}
		}
	}

	h.fs = fs
	h.path = path
	h.migrations = ms
	return nil
}

// Close is part of source.Driver interface implementation. This is a no-op.
func (h *Migrator) Close() error {
	return nil
}

// First is part of source.Driver interface implementation.
func (h *Migrator) First() (version uint, err error) {
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
func (h *Migrator) Prev(version uint) (prevVersion uint, err error) {
	if version, ok := h.migrations.Prev(version); ok {
		return version, nil
	}
	return 0, &os.PathError{
		Op:   "prev for version " + strconv.FormatUint(uint64(version), 10),
		Path: h.path,
		Err:  os.ErrNotExist,
	}
}

// Next is part of source.Driver interface implementation.
func (h *Migrator) Next(version uint) (nextVersion uint, err error) {
	if version, ok := h.migrations.Next(version); ok {
		return version, nil
	}
	return 0, &os.PathError{
		Op:   "next for version " + strconv.FormatUint(uint64(version), 10),
		Path: h.path,
		Err:  os.ErrNotExist,
	}
}

// ReadUp is part of source.Driver interface implementation.
func (h *Migrator) ReadUp(version uint) (r io.ReadCloser, identifier string, err error) {
	if m, ok := h.migrations.Up(version); ok {
		body, err := h.fs.Open(path.Join(h.path, m.Raw))
		if err != nil {
			return nil, "", err
		}
		return body, m.Identifier, nil
	}
	return nil, "", &os.PathError{
		Op:   "read up for version " + strconv.FormatUint(uint64(version), 10),
		Path: h.path,
		Err:  os.ErrNotExist,
	}
}

// ReadDown is part of source.Driver interface implementation.
func (h *Migrator) ReadDown(version uint) (r io.ReadCloser, identifier string, err error) {
	if m, ok := h.migrations.Down(version); ok {
		body, err := h.fs.Open(path.Join(h.path, m.Raw))
		if err != nil {
			return nil, "", err
		}
		return body, m.Identifier, nil
	}
	return nil, "", &os.PathError{
		Op:   "read down for version " + strconv.FormatUint(uint64(version), 10),
		Path: h.path,
		Err:  os.ErrNotExist,
	}
}
