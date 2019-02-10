//go:generate esc -o escfs/escfs.go -ignore escfs/escfs.go -pkg escfs -prefix escfs/ escfs/
package httpfs

import (
	"testing"

	"github.com/golang-migrate/migrate/v4/source"
	"github.com/golang-migrate/migrate/v4/source/httpfs/escfs"
	st "github.com/golang-migrate/migrate/v4/source/testing"
)

type ESCFS struct {
	HTTPFS
}

func (e *ESCFS) Open(base string) (source.Driver, error) {
	err := e.Initialize(escfs.Dir(false, base), &Config{})
	if err != nil {
		return nil, err
	}
	return e, nil
}

func TestEscFilesystem(t *testing.T) {
	escFs := &ESCFS{}
	d, err := escFs.Open("/Test/")
	if err != nil {
		t.Fatal(err)
	}

	st.Test(t, d)
}
