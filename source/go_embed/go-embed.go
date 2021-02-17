// +build go1.6

package goembed

import (
	"embed"
	"fmt"
	"io"
	"os"
	"path"

	"github.com/golang-migrate/migrate/v4/source"
)

func init() {
	source.Register("embeds", &File{})
}

func WithEmbed(dir string, fs embed.FS) (source.Driver, error) {
	f := &File{
		path:       dir,
		fs:         fs,
		migrations: source.NewMigrations(),
	}

	files, err := f.fs.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	for _, fi := range files {
		if !fi.IsDir() {
			m, err := source.DefaultParse(fi.Name())
			if err != nil {
				continue // ignore files that we can't parse
			}
			if !f.migrations.Append(m) {
				return nil, fmt.Errorf("unable to parse file %v", fi.Name())
			}
		}
	}

	return f, nil
}

type File struct {
	fs         embed.FS
	path       string
	migrations *source.Migrations
}

func (f *File) Open(dir string) (source.Driver, error) {
	return nil, fmt.Errorf("not yet implemented")
}

func (f *File) Close() error {
	// nothing do to here
	return nil
}

func (f *File) First() (version uint, err error) {
	if v, ok := f.migrations.First(); !ok {
		return 0, &os.PathError{Op: "first", Path: f.path, Err: os.ErrNotExist}
	} else {
		return v, nil
	}
}

func (f *File) Prev(version uint) (prevVersion uint, err error) {
	if v, ok := f.migrations.Prev(version); !ok {
		return 0, &os.PathError{Op: fmt.Sprintf("prev for version %v", version), Path: f.path, Err: os.ErrNotExist}
	} else {
		return v, nil
	}
}

func (f *File) Next(version uint) (nextVersion uint, err error) {
	if v, ok := f.migrations.Next(version); !ok {
		return 0, &os.PathError{Op: fmt.Sprintf("next for version %v", version), Path: f.path, Err: os.ErrNotExist}
	} else {
		return v, nil
	}
}

func (f *File) ReadUp(version uint) (r io.ReadCloser, identifier string, err error) {
	if m, ok := f.migrations.Up(version); ok {
		r, err := f.fs.Open(path.Join(f.path, m.Raw))
		if err != nil {
			return nil, "", err
		}
		return r, m.Identifier, nil
	}
	return nil, "", &os.PathError{Op: fmt.Sprintf("read version %v", version), Path: f.path, Err: os.ErrNotExist}
}

func (f *File) ReadDown(version uint) (r io.ReadCloser, identifier string, err error) {
	if m, ok := f.migrations.Down(version); ok {
		r, err := f.fs.Open(path.Join(f.path, m.Raw))
		if err != nil {
			return nil, "", err
		}
		return r, m.Identifier, nil
	}
	return nil, "", &os.PathError{Op: fmt.Sprintf("read version %v", version), Path: f.path, Err: os.ErrNotExist}
}
