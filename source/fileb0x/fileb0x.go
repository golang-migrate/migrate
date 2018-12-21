package fileb0x

import (
	"net/http"

	"github.com/golang-migrate/migrate/v4/source"
	"github.com/golang-migrate/migrate/v4/source/file"
)

func New(fileb0x http.FileSystem) source.Driver {
	// Usage:
	//
	// import (
	//   "example.com/org/package/static" // fileb0x-built
	// )
	//
	// driver := fileb0x.New(static.HTTP)
	return file.New(fileb0x)
}

func MustRegister(name string, fileb0x http.FileSystem) {
	source.Register(name, New(fileb0x))
}
