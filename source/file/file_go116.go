// +build go1.16

package file

import (
	"os"

	"github.com/golang-migrate/migrate/v4/source"
	"github.com/golang-migrate/migrate/v4/source/iofs"
)

type File struct {
	iofs.PartialDriver
	url  string
	path string
}

func (f *File) Open(url string) (source.Driver, error) {
	p, err := parseURL(url)
	if err != nil {
		return nil, err
	}
	nf := &File{
		url:  url,
		path: p,
	}
	if err := nf.Init(os.DirFS(p), "."); err != nil {
		return nil, err
	}
	return nf, nil
}
