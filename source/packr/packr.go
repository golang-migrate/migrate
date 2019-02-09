package packr

import (
	"bytes"
	"fmt"
	"github.com/gobuffalo/packr"
	"github.com/golang-migrate/migrate/v4/source"
	"io"
	"io/ioutil"
	"os"
)

const PackrName = "packr"

func init() {
	source.Register(PackrName, &Packr{})
}

type Packr struct {
	migrations *source.Migrations
	box        *packr.Box
	path       string
}

func (b *Packr) Open(url string) (source.Driver, error) {
	return nil, fmt.Errorf("not yet implemented, please use WithInstance")
}

func WithInstance(instance interface{}) (source.Driver, error) {
	if _, ok := instance.(*packr.Box); !ok {
		return nil, fmt.Errorf("expects *packr.Box")
	}
	bx := instance.(*packr.Box)

	driver := &Packr{
		box:        bx,
		migrations: source.NewMigrations(),
		path:       bx.Path,
	}

	bx.List()

	for _, fi := range bx.List() {
		m, err := source.DefaultParse(fi)
		if err != nil {
			continue // ignore files that we can't parse
		}

		if !driver.migrations.Append(m) {
			return nil, fmt.Errorf("unable to parse file %v", fi)
		}
	}

	return driver, nil
}

func (b *Packr) Close() error {
	return nil
}

func (b *Packr) First() (version uint, err error) {
	if v, ok := b.migrations.First(); !ok {
		return 0, &os.PathError{"first", b.path, os.ErrNotExist}
	} else {
		return v, nil
	}
}

func (b *Packr) Prev(version uint) (prevVersion uint, err error) {
	if v, ok := b.migrations.Prev(version); !ok {
		return 0, &os.PathError{fmt.Sprintf("prev for version %v", version), b.path, os.ErrNotExist}
	} else {
		return v, nil
	}
}

func (b *Packr) Next(version uint) (nextVersion uint, err error) {
	if v, ok := b.migrations.Next(version); !ok {
		return 0, &os.PathError{fmt.Sprintf("next for version %v", version), b.path, os.ErrNotExist}
	} else {
		return v, nil
	}
}

func (b *Packr) ReadUp(version uint) (r io.ReadCloser, identifier string, err error) {
	if m, ok := b.migrations.Up(version); ok {
		body, err := b.box.Find(m.Raw)
		if err != nil {
			return nil, "", err
		}
		return ioutil.NopCloser(bytes.NewReader(body)), m.Identifier, nil
	}
	return nil, "", &os.PathError{fmt.Sprintf("read version %v", version), b.path, os.ErrNotExist}
}

func (b *Packr) ReadDown(version uint) (r io.ReadCloser, identifier string, err error) {
	if m, ok := b.migrations.Down(version); ok {
		body, err := b.box.Find(m.Raw)
		if err != nil {
			return nil, "", err
		}
		return ioutil.NopCloser(bytes.NewReader(body)), m.Identifier, nil
	}
	return nil, "", &os.PathError{fmt.Sprintf("read version %v", version), b.path, os.ErrNotExist}
}
