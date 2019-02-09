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

// WithInstance creates HTTPFS instance that uses provided FileSystem to source
// files. config argument can be used to select a subdirectory as the root for
// sourcing all files.
func WithInstance(fs http.FileSystem, config *Config) (source.Driver, error) {
	dir, err := fs.Open(config.Dir)
	if err != nil {
		return nil, fmt.Errorf("can't open directory with migrations: %s", err)
	}

	files, err := dir.Readdir(-1)
	dir.Close()
	if err != nil {
		return nil, fmt.Errorf("can't read files in migrations directory: %s", err)
	}

	sort.Slice(files, func(i, j int) bool { return files[i].Name() < files[j].Name() })

	s := &HTTPFS{
		path:       config.Dir,
		fs:         fs,
		migrations: source.NewMigrations(),
	}

	for _, fi := range files {
		if fi.IsDir() {
			continue
		}

		m, err := source.DefaultParse(fi.Name())
		if err != nil {
			continue // ignore files that we can't parse
		}

		if !s.migrations.Append(m) {
			return nil, fmt.Errorf("unable to parse file %v", fi.Name())
		}
	}

	return s, nil
}

// Open is not implemented as many FileSystems cannot be opened or have
// custom initialization.
//
// Common use:
//   plainFileSystem, err := WithInstance(http.Dir(tmpDir), &Config{})
//
//   escEmbeddedFileSystem, err := WithInstance(FS(false), &Config{})
func (f *HTTPFS) Open(url string) (source.Driver, error) {
	return nil, fmt.Errorf("not implemented")
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
