// +build !go1.16

package file

import (
	"net/http"

	"github.com/golang-migrate/migrate/v4/source"
	"github.com/golang-migrate/migrate/v4/source/httpfs"
)

type File struct {
	httpfs.PartialDriver
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
	if err := nf.Init(http.Dir(p), ""); err != nil {
		return nil, err
	}
	return nf, nil
}
