package packr

import (
	"net/http"

	"github.com/golang-migrate/migrate/v4/source"
	"github.com/golang-migrate/migrate/v4/source/file"
)

func New(box http.FileSystem) source.Driver {
	// Usage:
	//
	// import (
	//   "github.com/gobuffalo/packr"
	// )
	//
	// box := packr.NewBox("./templates")
	// driver := packr.New(box)
	return file.New(box)
}

func MustRegister(name string, box http.FileSystem) {
	source.Register(name, New(box))
}
