package webdav

import (
	"context"
	"net/http"
	"os"

	"github.com/golang-migrate/migrate/v4/source"
	"github.com/golang-migrate/migrate/v4/source/file"

	"golang.org/x/net/webdav"
)

type WrapDAV struct {
	fs  webdav.FileSystem
	ctx context.Context
}

func (w *WrapDAV) Open(name string) (http.File, error) {
	return w.fs.OpenFile(w.ctx, name, os.O_RDONLY, 0)
}

func New(fs webdav.FileSystem, ctxes ...context.Context) source.Driver {
	// Usage:
	//
	// import (
	//   // for example, a fileb0x-built webdav
	//   "example.com/org/package/static"
	// )
	//
	// driver := webdav.New(static.FS)
	var ctx context.Context

	if len(ctxes) == 0 {
		ctx = context.Background()
	} else {
		ctx = ctxes[0]
	}

	return file.New(&WrapDAV{fs: fs, ctx: ctx})
}

func MustRegister(name string, fs webdav.FileSystem, ctxes ...context.Context) {
	source.Register(name, New(fs, ctxes...))
}
