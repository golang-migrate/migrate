package pkger

import (
	"fmt"
	"net/http"
	stdurl "net/url"

	"github.com/golang-migrate/migrate/v4/source"
	"github.com/golang-migrate/migrate/v4/source/httpfs"
	"github.com/markbates/pkger"
	"github.com/markbates/pkger/pkging"
)

func init() {
	source.Register("pkger", &Pkger{})
}

// Pkger is a source.Driver that reads migrations from instances of
// pkging.Pkger.
type Pkger struct {
	httpfs.PartialDriver
}

// Open implements source.Driver. The path component of url will be used as the
// relative location of migrations. The returned driver will use the package
// scoped pkger.Open to access migrations.  The relative root and any
// migrations must be added to the global pkger.Pkger instance by calling
// pkger.Apply. Refer to Pkger documentation for more information.
func (p *Pkger) Open(url string) (source.Driver, error) {
	u, err := stdurl.Parse(url)
	if err != nil {
		return nil, err
	}

	// wrap pkger to implement http.FileSystem.
	fs := fsFunc(func(name string) (http.File, error) {
		f, err := pkger.Open(name)
		if err != nil {
			return nil, err
		}
		return f.(http.File), nil
	})

	if err := p.Init(fs, u.Path); err != nil {
		return nil, fmt.Errorf("failed to init driver with relative path %q: %w", u.Path, err)
	}

	return p, nil
}

// WithInstance returns a source.Driver that is backed by an instance of
// pkging.Pkger. The relative location of migrations is indicated by path. The
// path must exist on the pkging.Pkger instance for the driver to initialize
// successfully.
func WithInstance(instance pkging.Pkger, path string) (source.Driver, error) {
	if instance == nil {
		return nil, fmt.Errorf("expected instance of pkging.Pkger")
	}

	// wrap pkger to implement http.FileSystem.
	fs := fsFunc(func(name string) (http.File, error) {
		f, err := instance.Open(name)
		if err != nil {
			return nil, err
		}
		return f.(http.File), nil
	})

	var p Pkger

	if err := p.Init(fs, path); err != nil {
		return nil, fmt.Errorf("failed to init driver with relative path %q: %w", path, err)
	}

	return &p, nil
}

type fsFunc func(name string) (http.File, error)

// Open implements http.FileSystem.
func (f fsFunc) Open(name string) (http.File, error) {
	return f(name)
}
