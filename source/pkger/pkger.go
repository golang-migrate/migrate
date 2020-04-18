package pkger

import (
	"fmt"
	"net/http"

	"github.com/golang-migrate/migrate/v4/source"
	"github.com/golang-migrate/migrate/v4/source/httpfs"
	"github.com/markbates/pkger/pkging"
)

func init() {
	source.Register("pkger", &driver{})
}

// Pkger is an implementation of http.FileSystem backed by an instance of
// pkging.Pkger.
type Pkger struct {
	pkging.Pkger

	// Path is the relative path location of the migrations. It is passed to
	// httpfs.PartialDriver.Init. If unset "/" is used as all paths are
	// absolute.
	Path string
}

// Open implements http.FileSystem.
func (p *Pkger) Open(name string) (http.File, error) {
	f, err := p.Pkger.Open(name)
	if err != nil {
		return nil, err
	}
	return f.(http.File), nil
}

type driver struct {
	httpfs.PartialDriver
}

// Open implements source.Driver. NOT IMPLEMENTED.
func (d *driver) Open(url string) (source.Driver, error) {
	return nil, fmt.Errorf("not yet implemented")
}

// WithInstance returns a source.Driver that is backed by an instance of Pkger.
func WithInstance(instance interface{}) (source.Driver, error) {
	if _, ok := instance.(*Pkger); !ok {
		return nil, fmt.Errorf("expects *Pkger")
	}

	p := instance.(*Pkger)

	if p.Path == "" {
		p.Path = "/"
	}

	var fs http.FileSystem
	var ds driver

	fs = p

	if err := ds.Init(fs, p.Path); err != nil {
		return nil, fmt.Errorf("failed to init: %w", err)
	}

	return &ds, nil
}
