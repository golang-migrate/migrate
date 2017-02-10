// Deprecated: package file is deprecated.
// Will be removed soon.
package file

import (
	"github.com/mattes/migrate/migrate/direction"
)

type MigrationFile struct {
	Version  uint64
	UpFile   *File
	DownFile *File
}

type File struct {
	Path      string
	FileName  string
	Version   uint64
	Name      string
	Content   []byte
	Direction direction.Direction
}
