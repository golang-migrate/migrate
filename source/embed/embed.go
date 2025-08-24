package embed

import (
	"embed"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path"
	"strconv"

	"github.com/golang-migrate/migrate/v4/source"
)

type Embed struct {
	FS         embed.FS
	migrations *source.Migrations
	path       string
}

// NewEmbed returns a new Driver using the embed.FS and a relative path.
func NewEmbed(fsys embed.FS, path string) (source.Driver, error) {
	var e Embed
	if err := e.Init(fsys, path); err != nil {
		return nil, fmt.Errorf("failed to init embed driver with path %s: %w", path, err)
	}
	return &e, nil
}

// Open is part of source.Driver interface implementation.
// Open cannot be called on the embed driver directly as it's designed to use embed.FS.
func (e *Embed) Open(url string) (source.Driver, error) {
	return nil, errors.New("Open() cannot be called on the embed driver")
}

// Init prepares Embed instance to read migrations from embed.FS and a relative path.
func (e *Embed) Init(fsys embed.FS, path string) error {
	entries, err := fs.ReadDir(fsys, path)
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
	}

	e.FS = fsys
	e.path = path
	e.migrations = ms
	return nil
}

// Close is part of source.Driver interface implementation.
func (e *Embed) Close() error {
	// Since embed.FS doesn't support Close(), this method is a no-op
	return nil
}

// First is part of source.Driver interface implementation.
func (e *Embed) First() (version uint, err error) {
	if version, ok := e.migrations.First(); ok {
		return version, nil
	}
	return 0, &fs.PathError{
		Op:   "first",
		Path: e.path,
		Err:  fs.ErrNotExist,
	}
}

// Prev is part of source.Driver interface implementation.
func (e *Embed) Prev(version uint) (prevVersion uint, err error) {
	if version, ok := e.migrations.Prev(version); ok {
		return version, nil
	}
	return 0, &fs.PathError{
		Op:   "prev for version " + strconv.FormatUint(uint64(version), 10),
		Path: e.path,
		Err:  fs.ErrNotExist,
	}
}

// Next is part of source.Driver interface implementation.
func (e *Embed) Next(version uint) (nextVersion uint, err error) {
	if version, ok := e.migrations.Next(version); ok {
		return version, nil
	}
	return 0, &fs.PathError{
		Op:   "next for version " + strconv.FormatUint(uint64(version), 10),
		Path: e.path,
		Err:  fs.ErrNotExist,
	}
}

// ReadUp is part of source.Driver interface implementation.
func (e *Embed) ReadUp(version uint) (r io.ReadCloser, identifier string, err error) {
	if m, ok := e.migrations.Up(version); ok {
		body, err := e.FS.ReadFile(path.Join(e.path, m.Raw))
		if err != nil {
			return nil, "", err
		}
		return io.NopCloser(&fileReader{data: body}), m.Identifier, nil
	}
	return nil, "", &fs.PathError{
		Op:   "read up for version " + strconv.FormatUint(uint64(version), 10),
		Path: e.path,
		Err:  fs.ErrNotExist,
	}
}

// ReadDown is part of source.Driver interface implementation.
func (e *Embed) ReadDown(version uint) (r io.ReadCloser, identifier string, err error) {
	if m, ok := e.migrations.Down(version); ok {
		body, err := e.FS.ReadFile(path.Join(e.path, m.Raw))
		if err != nil {
			return nil, "", err
		}
		return io.NopCloser(&fileReader{data: body}), m.Identifier, nil
	}
	return nil, "", &fs.PathError{
		Op:   "read down for version " + strconv.FormatUint(uint64(version), 10),
		Path: e.path,
		Err:  fs.ErrNotExist,
	}
}

// fileReader []byte to io.ReadCloser
type fileReader struct {
	data []byte
	pos  int
}

func (fr *fileReader) Read(p []byte) (n int, err error) {
	if fr.pos >= len(fr.data) {
		return 0, io.EOF
	}
	n = copy(p, fr.data[fr.pos:])
	fr.pos += n
	return n, nil
}

func (fr *fileReader) Close() error {
	// do nothing, as embed.FS does not require closing
	return nil
}
