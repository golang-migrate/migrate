// +build go1.16

package file

import (
	"os"

	"github.com/golang-migrate/migrate/v4/source"
	"github.com/golang-migrate/migrate/v4/source/iofs"
)

type file struct {
	iofs.Driver
	url  string
	path string
}

func (f *file) Open(url string) (source.Driver, error) {
	p, err := parseURL(url)
	if err != nil {
		return nil, err
	}
	nf := &file{
		url:  url,
		path: p,
	}
	if err := nf.Init(os.DirFS(p), "."); err != nil {
		return nil, err
	}
	return nf, nil
}
