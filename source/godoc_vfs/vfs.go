// Package godoc_vfs contains a driver that reads migrations from a virtual file
// system.
//
// Implementations of the filesystem interface that read from zip files and
// maps, as well as the definition of the filesystem interface can be found in
// the golang.org/x/tools/godoc/vfs package.
package godoc_vfs

import (
	"github.com/golang-migrate/migrate/v4/source"
	"github.com/golang-migrate/migrate/v4/source/httpfs"

	"golang.org/x/tools/godoc/vfs"
	vfs_httpfs "golang.org/x/tools/godoc/vfs/httpfs"
)

func init() {
	source.Register("godoc-vfs", &VFS{})
}

// VFS is an implementation of driver that returns migrations from a virtual
// file system.
type VFS struct {
	httpfs.PartialDriver
	fs   vfs.FileSystem
	path string
}

// Open implements the source.Driver interface for VFS.
//
// Calling this function panics, instead use the WithInstance function.
// See the package level documentation for an example.
func (b *VFS) Open(url string) (source.Driver, error) {
	panic("not implemented")
}

// WithInstance creates a new driver from a virtual file system.
// If a tree named searchPath exists in the virtual filesystem, WithInstance
// searches for migration files there.
// It defaults to "/".
func WithInstance(fs vfs.FileSystem, searchPath string) (source.Driver, error) {
	if searchPath == "" {
		searchPath = "/"
	}

	bn := &VFS{
		fs:   fs,
		path: searchPath,
	}

	if err := bn.Init(vfs_httpfs.New(fs), searchPath); err != nil {
		return nil, err
	}

	return bn, nil
}
