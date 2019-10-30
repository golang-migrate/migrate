package packr

import (
	"fmt"
	"io"
	"io/ioutil"
	nurl "net/url"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/gobuffalo/packr/v2"
	"github.com/golang-migrate/migrate/v4/source"
)

func init() {
	source.Register("packr", &Packr{})
}

type Packr struct {
	box        *packr.Box
	url        string
	path       string
	migrations *source.Migrations
}

func (p *Packr) Open(url string) (source.Driver, error) {
	u, err := nurl.Parse(url)
	if err != nil {
		return nil, err
	}

	path := filepath.Clean(u.Path)
	if len(path) > 0 && path[0] == filepath.Separator {
		path = path[1:]
	}

	box := packr.New(u.Host, path)

	np := &Packr{
		url:        url,
		path:       path,
		migrations: source.NewMigrations(),
		box:        box,
	}

	err = box.WalkPrefix(path, func(fpath string, f packr.File) error {
		// discard if there is more levels
		fdir, fname := filepath.Split(fpath)
		if fdir[:len(path)] != path {
			return nil
		}

		m, err := source.DefaultParse(fname)
		if err != nil {
			return nil // ignore files that we can't parse
		}
		if !np.migrations.Append(m) {
			return fmt.Errorf("unable to parse file %v", fname)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return np, nil
}

func (p *Packr) Close() error {
	// nothing do to here
	return nil
}

func (p *Packr) First() (version uint, err error) {
	if v, ok := p.migrations.First(); ok {
		return v, nil
	}
	return 0, &os.PathError{Op: "first", Path: p.path, Err: os.ErrNotExist}
}

func (p *Packr) Prev(version uint) (prevVersion uint, err error) {
	if v, ok := p.migrations.Prev(version); ok {
		return v, nil
	}
	return 0, &os.PathError{Op: fmt.Sprintf("prev for version %v", version), Path: p.path, Err: os.ErrNotExist}
}

func (p *Packr) Next(version uint) (nextVersion uint, err error) {
	if v, ok := p.migrations.Next(version); ok {
		return v, nil
	}
	return 0, &os.PathError{Op: fmt.Sprintf("next for version %v", version), Path: p.path, Err: os.ErrNotExist}
}

func (p *Packr) ReadUp(version uint) (r io.ReadCloser, identifier string, err error) {
	if m, ok := p.migrations.Up(version); ok {
		r, err = p.read(m.Raw)
		return r, m.Identifier, nil
	}
	return nil, "", &os.PathError{Op: fmt.Sprintf("read version %v", version), Path: p.path, Err: os.ErrNotExist}
}

func (p *Packr) ReadDown(version uint) (r io.ReadCloser, identifier string, err error) {
	if m, ok := p.migrations.Down(version); ok {
		r, err = p.read(m.Raw)
		if err != nil {
			return nil, "", err
		}
		return r, m.Identifier, nil
	}
	return nil, "", &os.PathError{Op: fmt.Sprintf("read version %v", version), Path: p.path, Err: os.ErrNotExist}
}

func (p *Packr) read(fname string) (io.ReadCloser, error) {
	s, err := p.box.FindString(path.Join(p.path, fname))
	if err != nil {
		return nil, err
	}
	r := ioutil.NopCloser(strings.NewReader(s))
	return r, nil
}

var _ = func() {
	packr.New("testdata", "./testdata")
}
