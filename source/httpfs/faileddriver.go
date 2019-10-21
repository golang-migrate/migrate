package httpfs

import (
	"io"

	"github.com/golang-migrate/migrate/v4/source"
)

// failedDriver is a dummy implementation of source.Driver interface that
// always returns underlying error.
type failedDriver struct {
	err error
}

func (d *failedDriver) Open(url string) (source.Driver, error) {
	return nil, d.err
}

func (d *failedDriver) Close() error {
	return d.err
}

func (d *failedDriver) First() (version uint, err error) {
	return 0, d.err
}

func (d *failedDriver) Prev(version uint) (prevVersion uint, err error) {
	return 0, d.err
}

func (d *failedDriver) Next(version uint) (nextVersion uint, err error) {
	return 0, d.err
}

func (d *failedDriver) ReadUp(version uint) (r io.ReadCloser, identifier string, err error) {
	return nil, "", d.err
}

func (d *failedDriver) ReadDown(version uint) (r io.ReadCloser, identifier string, err error) {
	return nil, "", d.err
}
