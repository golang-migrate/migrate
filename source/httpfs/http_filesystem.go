package httpfs

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"sort"

	"github.com/golang-migrate/migrate/v4/source"
)

// Config object.
// Dir - base directory for serving files
type Config struct {
	Dir string
}

// HTTPFS is migration source that uses http.FileSystem as the source of
// migration scripts.
type HTTPFS struct {
	path       string
	fs         http.FileSystem
	migrations *source.Migrations
}

// Initialize initializes HTTPFS instance to use provided FileSystem to source
// files. config argument can be used to select a subdirectory as the root for
// sourcing all files.
func (f *HTTPFS) Initialize(fs http.FileSystem, config *Config) error {
	dir, err := fs.Open(config.Dir)
	if err != nil {
		return fmt.Errorf("can't open directory with migrations: %s", err)
	}

	files, err := dir.Readdir(-1)
	dir.Close()
	if err != nil {
		return fmt.Errorf("can't read files in migrations directory: %s", err)
	}

	sort.Slice(files, func(i, j int) bool { return files[i].Name() < files[j].Name() })

	migrations := source.NewMigrations()

	for _, fi := range files {
		if fi.IsDir() {
			continue
		}

		m, err := source.DefaultParse(fi.Name())
		if err != nil {
			continue // ignore files that we can't parse
		}

		if !migrations.Append(m) {
			return fmt.Errorf("unable to parse file %v", fi.Name())
		}
	}

	f.path = config.Dir
	f.fs = fs
	f.migrations = migrations

	return nil
}

// Close currently does not do anything, as there is no common way to release
// FileSystem objects.
func (f *HTTPFS) Close() error {
	// nothing do to here
	return nil
}

// First is implementation of source.First(). Please see it for documentation.
func (f *HTTPFS) First() (version uint, err error) {
	v, ok := f.migrations.First()
	if !ok {
		return 0, &os.PathError{Op: "first", Path: f.path, Err: os.ErrNotExist}
	}

	return v, nil
}

// Prev is implementation of source.Prev(). Please see it for documentation.
func (f *HTTPFS) Prev(version uint) (prevVersion uint, err error) {
	v, ok := f.migrations.Prev(version)
	if !ok {
		return 0, &os.PathError{Op: fmt.Sprintf("prev for version %v", version), Path: f.path, Err: os.ErrNotExist}
	}

	return v, nil
}

// Next is implementation of source.Next(). Please see it for documentation.
func (f *HTTPFS) Next(version uint) (nextVersion uint, err error) {
	v, ok := f.migrations.Next(version)
	if !ok {
		return 0, &os.PathError{Op: fmt.Sprintf("next for version %v", version), Path: f.path, Err: os.ErrNotExist}
	}

	return v, nil
}

// ReadUp is implementation of source.ReadUp(). Please see it for documentation.
func (f *HTTPFS) ReadUp(version uint) (r io.ReadCloser, identifier string, err error) {
	if m, ok := f.migrations.Up(version); ok {
		p := path.Join(f.path, m.Raw)
		r, err := f.fs.Open(p)
		if err != nil {
			return nil, "", fmt.Errorf("can't open file %s: %s", p, err)
		}

		return r, m.Identifier, nil
	}

	return nil, "", &os.PathError{Op: fmt.Sprintf("read version %v", version), Path: f.path, Err: os.ErrNotExist}
}

// ReadDown is implementation of source.ReadDown(). Please see it for documentation.
func (f *HTTPFS) ReadDown(version uint) (r io.ReadCloser, identifier string, err error) {
	if m, ok := f.migrations.Down(version); ok {
		p := path.Join(f.path, m.Raw)
		r, err := f.fs.Open(p)
		if err != nil {
			return nil, "", fmt.Errorf("can't open file %s: %s", p, err)
		}

		return r, m.Identifier, nil
	}

	return nil, "", &os.PathError{Op: fmt.Sprintf("read version %v", version), Path: f.path, Err: os.ErrNotExist}
}
