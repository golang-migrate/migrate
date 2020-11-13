// +build go1.16

package iofs

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path"
	"strconv"

	"github.com/golang-migrate/migrate/v4/source"
)

func init() {
	source.Register("iofs", &Iofs{})
}

// Iofs is a source driver for io/fs#FS.
type Iofs struct {
	migrations *source.Migrations
	fsys       fs.ReadDirFS
	path       string
}

// Open by url does not supported with Iofs.
func (i *Iofs) Open(url string) (source.Driver, error) {
	return nil, errors.New("iofs driver does not support open by url")
}

// WithInstance wraps io/fs#FS as source.Driver.
func WithInstance(fsys fs.ReadDirFS, path string) (source.Driver, error) {
	var i Iofs
	if err := i.Init(fsys, path); err != nil {
		return nil, fmt.Errorf("failed to init driver with path %s: %w", path, err)
	}
	return &i, nil
}

// Init prepares not initialized Iofs instance to read migrations from a
// fs.ReadDirFS instance and a relative path.
func (p *Iofs) Init(fsys fs.ReadDirFS, path string) error {
	entries, err := fsys.ReadDir(path)
	if err != nil {
		return err
	}

	ms := source.NewMigrations()
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		m, err := source.DefaultParse(e.Name())
		if err != nil {
			continue
		}
		if !ms.Append(m) {
			return source.ErrDuplicateMigration{
				Migration: *m,
				FileInfo:  e,
			}
		}
	}

	p.fsys = fsys
	p.path = path
	p.migrations = ms
	return nil
}

// Close is part of source.Driver interface implementation. This is a no-op.
func (p *Iofs) Close() error {
	return nil
}

// First is part of source.Driver interface implementation.
func (p *Iofs) First() (version uint, err error) {
	if version, ok := p.migrations.First(); ok {
		return version, nil
	}
	return 0, &fs.PathError{
		Op:   "first",
		Path: p.path,
		Err:  fs.ErrNotExist,
	}
}

// Prev is part of source.Driver interface implementation.
func (p *Iofs) Prev(version uint) (prevVersion uint, err error) {
	if version, ok := p.migrations.Prev(version); ok {
		return version, nil
	}
	return 0, &fs.PathError{
		Op:   "prev for version " + strconv.FormatUint(uint64(version), 10),
		Path: p.path,
		Err:  fs.ErrNotExist,
	}
}

// Next is part of source.Driver interface implementation.
func (p *Iofs) Next(version uint) (nextVersion uint, err error) {
	if version, ok := p.migrations.Next(version); ok {
		return version, nil
	}
	return 0, &fs.PathError{
		Op:   "next for version " + strconv.FormatUint(uint64(version), 10),
		Path: p.path,
		Err:  fs.ErrNotExist,
	}
}

// ReadUp is part of source.Driver interface implementation.
func (p *Iofs) ReadUp(version uint) (r io.ReadCloser, identifier string, err error) {
	if m, ok := p.migrations.Up(version); ok {
		body, err := p.open(path.Join(p.path, m.Raw))
		if err != nil {
			return nil, "", err
		}
		return body, m.Identifier, nil
	}
	return nil, "", &fs.PathError{
		Op:   "read up for version " + strconv.FormatUint(uint64(version), 10),
		Path: p.path,
		Err:  fs.ErrNotExist,
	}
}

// ReadDown is part of source.Driver interface implementation.
func (p *Iofs) ReadDown(version uint) (r io.ReadCloser, identifier string, err error) {
	if m, ok := p.migrations.Down(version); ok {
		body, err := p.open(path.Join(p.path, m.Raw))
		if err != nil {
			return nil, "", err
		}
		return body, m.Identifier, nil
	}
	return nil, "", &fs.PathError{
		Op:   "read down for version " + strconv.FormatUint(uint64(version), 10),
		Path: p.path,
		Err:  fs.ErrNotExist,
	}
}

func (p *Iofs) open(path string) (fs.File, error) {
	f, err := p.fsys.Open(path)
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
