// Deprecated: package migrate is here to make sure v2 is downwards compatible with v1
package migrate

import (
	"fmt"

	"github.com/mattes/migrate"
	"github.com/mattes/migrate/migrate/file"
)

var deprecatedMessage = "You are using a deprecated version of migrate, update at https://github.com/mattes/migrate"

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

func UpSync(url, migrationsPath string) (err []error, ok bool) {
	return nil, false
}

func Down(pipe chan interface{}, url, migrationsPath string) {
}

func DownSync(url, migrationsPath string) (err []error, ok bool) {
	return nil, false
}

func Redo(pipe chan interface{}, url, migrationsPath string) {
}

func RedoSync(url, migrationsPath string) (err []error, ok bool) {
	return nil, false
}

func Reset(pipe chan interface{}, url, migrationsPath string) {
}

func ResetSync(url, migrationsPath string) (err []error, ok bool) {
	return nil, false
}

func Migrate(pipe chan interface{}, url, migrationsPath string, relativeN int) {

}

func MigrateSync(url, migrationsPath string, relativeN int) (err []error, ok bool) {
	return nil, false
}

func Version(url, migrationsPath string) (version uint64, err error) {
	return 0, nil
}

func Create(url, migrationsPath, name string) (*file.MigrationFile, error) {
	return nil, fmt.Errorf(deprecatedMessage)
}

func NewPipe() chan interface{} {
	return nil
}

func Graceful() {}

func NonGraceful() {}
