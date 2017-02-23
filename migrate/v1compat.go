// Deprecated: package migrate makes this version backwards compatible.
// Expect this to be removed very soon.
package migrate

import (
	"fmt"

	"github.com/mattes/migrate"
	"github.com/mattes/migrate/migrate/file"
)

var deprecatedMessage = ""

func Up(pipe chan interface{}, url, migrationsPath string) {
	m, err := migrate.New("file://"+migrationsPath, url)
	if err != nil {
		pipe <- err
		return
	}

	if err := m.Up(); err != nil {
		pipe <- err
	}
}

func UpSync(url, migrationsPath string) (errs []error, ok bool) {
	m, err := migrate.New("file://"+migrationsPath, url)
	if err != nil {
		return []error{err}, false
	}
	if err := m.Up(); err != nil {
		return []error{err}, false
	}
	return nil, true
}

func Down(pipe chan interface{}, url, migrationsPath string) {
	m, err := migrate.New("file://"+migrationsPath, url)
	if err != nil {
		pipe <- err
		return
	}

	if err := m.Down(); err != nil {
		pipe <- err
	}
}

func DownSync(url, migrationsPath string) (errs []error, ok bool) {
	m, err := migrate.New("file://"+migrationsPath, url)
	if err != nil {
		return []error{err}, false
	}
	if err := m.Down(); err != nil {
		return []error{err}, false
	}
	return nil, true
}

func Redo(pipe chan interface{}, url, migrationsPath string) {
	m, err := migrate.New("file://"+migrationsPath, url)
	if err != nil {
		pipe <- err
		return
	}

	if err := m.Steps(-1); err != nil {
		pipe <- err
		return
	}

	if err := m.Steps(1); err != nil {
		pipe <- err
	}
}

func RedoSync(url, migrationsPath string) (errs []error, ok bool) {
	m, err := migrate.New("file://"+migrationsPath, url)
	if err != nil {
		return []error{err}, false
	}
	if err := m.Steps(-1); err != nil {
		return []error{err}, false
	}
	if err := m.Steps(1); err != nil {
		return []error{err}, false
	}
	return nil, true
}

func Reset(pipe chan interface{}, url, migrationsPath string) {
	m, err := migrate.New("file://"+migrationsPath, url)
	if err != nil {
		pipe <- err
		return
	}

	if err := m.Drop(); err != nil {
		pipe <- err
		return
	}

	if err := m.Up(); err != nil {
		pipe <- err
	}
}

func ResetSync(url, migrationsPath string) (errs []error, ok bool) {
	m, err := migrate.New("file://"+migrationsPath, url)
	if err != nil {
		return []error{err}, false
	}
	if err := m.Drop(); err != nil {
		return []error{err}, false
	}
	if err := m.Up(); err != nil {
		return []error{err}, false
	}
	return nil, true
}

func Migrate(pipe chan interface{}, url, migrationsPath string, relativeN int) {
	m, err := migrate.New("file://"+migrationsPath, url)
	if err != nil {
		pipe <- err
		return
	}

	if err := m.Steps(relativeN); err != nil {
		pipe <- err
	}
}

func MigrateSync(url, migrationsPath string, relativeN int) (errs []error, ok bool) {
	m, err := migrate.New("file://"+migrationsPath, url)
	if err != nil {
		return []error{err}, false
	}
	if err := m.Steps(relativeN); err != nil {
		return []error{err}, false
	}
	return nil, true
}

func Version(url, migrationsPath string) (version uint64, err error) {
	m, err := migrate.New("file://"+migrationsPath, url)
	if err != nil {
		return 0, err
	}

	v, _, err := m.Version()
	if err != nil {
		return 0, err
	}
	return uint64(v), nil
}

func Create(url, migrationsPath, name string) (*file.MigrationFile, error) {
	return nil, fmt.Errorf("You are using a deprecated version of migrate, update at https://github.com/mattes/migrate")
}

func NewPipe() chan interface{} {
	return make(chan interface{}, 0)
}

func Graceful() {}

func NonGraceful() {}
