package packr

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"

	"github.com/golang-migrate/migrate/v4/source"

	packr "github.com/gobuffalo/packr/v2"
	"github.com/gobuffalo/packr/v2/file"
)

type Packr struct {
	box        *packr.Box
	migrations *source.Migrations
}

func (p *Packr) Open(url string) (source.Driver, error) {
	// TODO find a way to get a box instance by reflection
	return nil, errors.New("Not implemented")
}

func (p *Packr) Close() error {
	return nil
}

func (p *Packr) First() (version uint, err error) {
	if v, ok := p.migrations.First(); ok {
		return v, nil
	}
	return 0, &os.PathError{Op: "first", Path: p.box.Path, Err: os.ErrNotExist}
}

func (p *Packr) Prev(version uint) (prevVersion uint, err error) {
	if v, ok := p.migrations.Prev(version); ok {
		return v, nil
	}
	return 0, &os.PathError{Op: fmt.Sprintf("prev for version %v", version), Path: p.box.Path, Err: os.ErrNotExist}
}

func (p *Packr) Next(version uint) (nextVersion uint, err error) {
	if v, ok := p.migrations.Next(version); ok {
		return v, nil
	}
	return 0, &os.PathError{Op: fmt.Sprintf("next for version %v", version), Path: p.box.Path, Err: os.ErrNotExist}
}

func (p *Packr) ReadUp(version uint) (r io.ReadCloser, identifier string, err error) {
	if m, ok := p.migrations.Up(version); ok {
		mb, err := p.box.Find(m.Raw)
		if err != nil {
			return nil, "", err
		}
		return ioutil.NopCloser(bytes.NewBuffer(mb)), m.Identifier, nil
	}
	return nil, "", &os.PathError{Op: fmt.Sprintf("read version %v", version), Path: p.box.Path, Err: os.ErrNotExist}
}

func (p *Packr) ReadDown(version uint) (r io.ReadCloser, identifier string, err error) {
	if m, ok := p.migrations.Down(version); ok {
		mb, err := p.box.Find(m.Raw)
		if err != nil {
			return nil, "", err
		}
		return ioutil.NopCloser(bytes.NewBuffer(mb)), m.Identifier, nil
	}
	return nil, "", &os.PathError{Op: fmt.Sprintf("read version %v", version), Path: p.box.Path, Err: os.ErrNotExist}
}

func WithInstance(box *packr.Box) (source.Driver, error) {
	p := &Packr{
		box:        box,
		migrations: source.NewMigrations(),
	}

	err := box.Walk(func(path string, info file.File) error {
		if info == nil {
			return nil
		}
		finfo, _ := info.FileInfo()
		if !finfo.IsDir() {
			m, err := source.DefaultParse(finfo.Name())
			if err != nil {
				// Ignore files we can't parse
				return nil
			}
			if !p.migrations.Append(m) {
				return fmt.Errorf("unable to parse file %v", finfo.Name())
			}
		}
		return nil
	})
	return p, err
}
