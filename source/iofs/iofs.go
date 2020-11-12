// +build go1.16

package iofs

import (
	"errors"
	"fmt"
	"io/fs"
	"net/http"

	"github.com/golang-migrate/migrate/v4/source"
	"github.com/golang-migrate/migrate/v4/source/httpfs"
)

func init() {
	source.Register("iofs", &Iofs{})
}

// Iofs is a source driver for io/fs#FS.
type Iofs struct {
	httpfs.PartialDriver
}

// Open by url does not supported with Iofs.
func (i *Iofs) Open(url string) (source.Driver, error) {
	return nil, errors.New("iofs driver does not support open by url")
}

// WithInstance wraps io/fs#FS as source.Driver.
func WithInstance(fsys fs.FS, path string) (source.Driver, error) {
	var i Iofs
	if err := i.Init(http.FS(fsys), path); err != nil {
		return nil, fmt.Errorf("failed to init driver with path %s: %w", path, err)
	}
	return &i, nil
}
