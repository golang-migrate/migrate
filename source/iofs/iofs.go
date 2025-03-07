//go:build go1.16
// +build go1.16

package iofs

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path"
	"strconv"
	"strings"

	"github.com/golang-migrate/migrate/v4/source"
)

type driver struct {
	PartialDriver
}

// New returns a new Driver from io/fs#FS and a relative path.
func New(fsys fs.FS, path string) (source.Driver, error) {
	var i driver
	if err := i.Init(fsys, path); err != nil {
		return nil, fmt.Errorf("failed to init driver with path %s: %w", path, err)
	}
	return &i, nil
}

// Open is part of source.Driver interface implementation.
// Open cannot be called on the iofs passthrough driver.
func (d *driver) Open(url string) (source.Driver, error) {
	return nil, errors.New("Open() cannot be called on the iofs passthrough driver")
}

// PartialDriver is a helper service for creating new source drivers working with
// io/fs.FS instances. It implements all source.Driver interface methods
// except for Open(). New driver could embed this struct and add missing Open()
// method.
//
// To prepare PartialDriver for use Init() function.
type PartialDriver struct {
	migrations *source.Migrations
	fsys       fs.FS
	path       string

	isRecursive   bool
	migrationsMap map[string]string
}

// RecursiveSuffix for search recursive in paths, base path must be ended by this suffix.
const RecursiveSuffix = "/*"

func concatPath(path, suffix string) string {
	if suffix != "" {
		path = fmt.Sprintf("%s/%s", path, suffix)
	}

	return path
}

func (d *PartialDriver) getRawPath(raw string) string {
	if d.isRecursive {
		raw = concatPath(d.migrationsMap[raw], raw)
	}

	return raw
}

func (d *PartialDriver) recursivePath(fsys fs.FS, path, suffix string, ms *source.Migrations) error {
	entries, err := fs.ReadDir(fsys, concatPath(path, suffix))
	if err != nil {
		return err
	}

	for _, e := range entries {
		if e.IsDir() {
			if d.isRecursive {
				rSuffix := e.Name()

				if suffix != "" {
					rSuffix = concatPath(suffix, e.Name())
				}

				if err = d.recursivePath(fsys, path, rSuffix, ms); err != nil {
					return err
				}
			} else {
				continue
			}
		}

		m, err := source.DefaultParse(e.Name())
		if err != nil {
			continue
		}
		file, err := e.Info()
		if err != nil {
			return err
		}
		if !ms.Append(m) {
			return source.ErrDuplicateMigration{
				Migration: *m,
				FileInfo:  file,
			}
		}

		if d.isRecursive {
			d.migrationsMap[e.Name()] = suffix
		}
	}

	return nil
}

// Init prepares not initialized IoFS instance to read migrations from a
// io/fs#FS instance and a relative path.
func (d *PartialDriver) Init(fsys fs.FS, path string) error {
	if strings.HasSuffix(path, RecursiveSuffix) {
		path = strings.TrimSuffix(path, RecursiveSuffix)
		d.isRecursive = true
		d.migrationsMap = make(map[string]string)
	}

	ms := source.NewMigrations()

	if err := d.recursivePath(fsys, path, "", ms); err != nil {
		return err
	}

	d.fsys = fsys
	d.path = path
	d.migrations = ms
	return nil
}

// Close is part of source.Driver interface implementation.
// Closes the file system if possible.
func (d *PartialDriver) Close() error {
	c, ok := d.fsys.(io.Closer)
	if !ok {
		return nil
	}
	return c.Close()
}

// First is part of source.Driver interface implementation.
func (d *PartialDriver) First() (version uint, err error) {
	if version, ok := d.migrations.First(); ok {
		return version, nil
	}
	return 0, &fs.PathError{
		Op:   "first",
		Path: d.path,
		Err:  fs.ErrNotExist,
	}
}

// Prev is part of source.Driver interface implementation.
func (d *PartialDriver) Prev(version uint) (prevVersion uint, err error) {
	if version, ok := d.migrations.Prev(version); ok {
		return version, nil
	}
	return 0, &fs.PathError{
		Op:   "prev for version " + strconv.FormatUint(uint64(version), 10),
		Path: d.path,
		Err:  fs.ErrNotExist,
	}
}

// Next is part of source.Driver interface implementation.
func (d *PartialDriver) Next(version uint) (nextVersion uint, err error) {
	if version, ok := d.migrations.Next(version); ok {
		return version, nil
	}
	return 0, &fs.PathError{
		Op:   "next for version " + strconv.FormatUint(uint64(version), 10),
		Path: d.path,
		Err:  fs.ErrNotExist,
	}
}

// ReadUp is part of source.Driver interface implementation.
func (d *PartialDriver) ReadUp(version uint) (r io.ReadCloser, identifier string, err error) {
	if m, ok := d.migrations.Up(version); ok {
		body, err := d.open(path.Join(d.path, d.getRawPath(m.Raw)))
		if err != nil {
			return nil, "", err
		}
		return body, m.Identifier, nil
	}
	return nil, "", &fs.PathError{
		Op:   "read up for version " + strconv.FormatUint(uint64(version), 10),
		Path: d.path,
		Err:  fs.ErrNotExist,
	}
}

// ReadDown is part of source.Driver interface implementation.
func (d *PartialDriver) ReadDown(version uint) (r io.ReadCloser, identifier string, err error) {
	if m, ok := d.migrations.Down(version); ok {
		body, err := d.open(path.Join(d.path, d.getRawPath(m.Raw)))
		if err != nil {
			return nil, "", err
		}
		return body, m.Identifier, nil
	}
	return nil, "", &fs.PathError{
		Op:   "read down for version " + strconv.FormatUint(uint64(version), 10),
		Path: d.path,
		Err:  fs.ErrNotExist,
	}
}

func (d *PartialDriver) open(path string) (fs.File, error) {
	f, err := d.fsys.Open(path)
	if err == nil {
		return f, nil
	}
	// Some non-standard file systems may return errors that don't include the path, that
	// makes debugging harder.
	if !errors.As(err, new(*fs.PathError)) {
		err = &fs.PathError{
			Op:   "open",
			Path: path,
			Err:  err,
		}
	}
	return nil, err
}
