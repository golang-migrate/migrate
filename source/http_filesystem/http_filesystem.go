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

func init() {
	source.Register("http-fs", &httpFs{})
}

type Config struct {
	Dir string
}

type httpFs struct {
	path       string
	fs         http.FileSystem
	migrations *source.Migrations
}

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

	s := &httpFs{
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

func (f *httpFs) Open(url string) (source.Driver, error) {
	return nil, fmt.Errorf("not implemented")
}

func (f *httpFs) Close() error {
	// nothing do to here
	return nil
}

func (f *httpFs) First() (version uint, err error) {
	v, ok := f.migrations.First()
	if !ok {
		return 0, &os.PathError{Op: "first", Path: f.path, Err: os.ErrNotExist}
	}

	return v, nil
}

func (f *httpFs) Prev(version uint) (prevVersion uint, err error) {
	v, ok := f.migrations.Prev(version)
	if !ok {
		return 0, &os.PathError{Op: fmt.Sprintf("prev for version %v", version), Path: f.path, Err: os.ErrNotExist}
	}

	return v, nil
}

func (f *httpFs) Next(version uint) (nextVersion uint, err error) {
	v, ok := f.migrations.Next(version)
	if !ok {
		return 0, &os.PathError{Op: fmt.Sprintf("next for version %v", version), Path: f.path, Err: os.ErrNotExist}
	}

	return v, nil
}

func (f *httpFs) ReadUp(version uint) (r io.ReadCloser, identifier string, err error) {
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

func (f *httpFs) ReadDown(version uint) (r io.ReadCloser, identifier string, err error) {
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
