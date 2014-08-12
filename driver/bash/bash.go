package bash

import (
	"github.com/mattes/migrate/file"
	_ "github.com/mattes/migrate/migrate/direction"
)

type Driver struct {
}

func (driver *Driver) Initialize(url string) error {
	return nil
}

func (driver *Driver) FilenameExtension() string {
	return "sh"
}

func (driver *Driver) Migrate(files file.Files, pipe chan interface{}) {
	defer close(pipe)
	return
}

func (driver *Driver) Version() (uint64, error) {
	return uint64(0), nil
}
