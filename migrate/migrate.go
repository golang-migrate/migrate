// Package migrate is imported by other Go code.
// It is the entry point to all migration functions.
package migrate

import (
	"fmt"
	"github.com/mattes/migrate/driver"
	"github.com/mattes/migrate/file"
	"github.com/mattes/migrate/migrate/direction"
	pipep "github.com/mattes/migrate/pipe"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"strings"
)

// // Up applies all available migrations
// func Up(pipe chan interface{}, url, migrationsPath string) {
// 	d, files, version, err := initDriverAndReadMigrationFilesAndGetVersion(url, migrationsPath)
// 	if err != nil {
// 		go pipep.Close(pipe, err)
// 		return
// 	}

// 	applyMigrationFiles, err := files.ToLastFrom(version)
// 	if err != nil {
// 		go pipep.Close(pipe, err)
// 		return
// 	}

// 	if len(applyMigrationFiles) > 0 {
// 		for _, f := range applyMigrationFiles {
// 			pipe1 := pipep.New()
// 			go d.Migrate(f, pipe1)
// 			if ok := pipep.WaitAndRedirect(pipe1, pipe); !ok {
// 				break
// 			}
// 		}
// 		go pipep.Close(pipe, nil)
// 		return
// 	} else {
// 		go pipep.Close(pipe, nil)
// 		return
// 	}
// }

func Up(pipe chan interface{}, signal chan os.Signal, url, migrationsPath string) {
	d, files, version, err := initDriverAndReadMigrationFilesAndGetVersion(url, migrationsPath)
	if err != nil {
		go pipep.Close(pipe, err)
		return
	}

	applyMigrationFiles, err := files.ToLastFrom(version)
	if err != nil {
		go pipep.Close(pipe, err)
		return
	}

	if len(applyMigrationFiles) > 0 {
		for _, f := range applyMigrationFiles {
			pipe1 := pipep.New()
			go d.Migrate(f, pipe1)
			if ok := pipep.WaitAndRedirect(pipe1, pipe, signal); !ok {
				break
			}
		}
		go pipep.Close(pipe, nil)
		return
	} else {
		go pipep.Close(pipe, nil)
		return
	}
}

// UpSync is synchronous version of Up
func UpSync(url, migrationsPath string) (err []error, ok bool) {
	pipe := pipep.New()
	go Up(pipe, pipep.Signal(), url, migrationsPath)
	err = pipep.ReadErrors(pipe)
	return err, len(err) == 0
}

// Down rolls back all migrations
func Down(pipe chan interface{}, signal chan os.Signal, url, migrationsPath string) {
	d, files, version, err := initDriverAndReadMigrationFilesAndGetVersion(url, migrationsPath)
	if err != nil {
		go pipep.Close(pipe, err)
		return
	}

	applyMigrationFiles, err := files.ToFirstFrom(version)
	if err != nil {
		go pipep.Close(pipe, err)
		return
	}

	if len(applyMigrationFiles) > 0 {
		for _, f := range applyMigrationFiles {
			pipe1 := pipep.New()
			go d.Migrate(f, pipe1)
			if ok := pipep.WaitAndRedirect(pipe1, pipe, signal); !ok {
				break
			}
		}
		go pipep.Close(pipe, nil)
		return
	} else {
		go pipep.Close(pipe, nil)
		return
	}
}

// DownSync is synchronous version of Down
func DownSync(url, migrationsPath string) (err []error, ok bool) {
	pipe := pipep.New()
	go Down(pipe, pipep.Signal(), url, migrationsPath)
	err = pipep.ReadErrors(pipe)
	return err, len(err) == 0
}

// Redo rolls back the most recently applied migration, then runs it again.
func Redo(pipe chan interface{}, signal chan os.Signal, url, migrationsPath string) {
	pipe1 := pipep.New()
	go Migrate(pipe1, signal, url, migrationsPath, -1)
	if ok := pipep.WaitAndRedirect(pipe1, pipe, signal); !ok {
		go pipep.Close(pipe, nil)
		return
	} else {
		go Migrate(pipe, pipep.Signal(), url, migrationsPath, +1)
	}
}

// RedoSync is synchronous version of Redo
func RedoSync(url, migrationsPath string) (err []error, ok bool) {
	pipe := pipep.New()
	go Redo(pipe, pipep.Signal(), url, migrationsPath)
	err = pipep.ReadErrors(pipe)
	return err, len(err) == 0
}

// Reset runs the down and up migration function
func Reset(pipe chan interface{}, signal chan os.Signal, url, migrationsPath string) {
	pipe1 := pipep.New()
	go Down(pipe1, signal, url, migrationsPath)
	if ok := pipep.WaitAndRedirect(pipe1, pipe, signal); !ok {
		go pipep.Close(pipe, nil)
		return
	} else {
		go Up(pipe, signal, url, migrationsPath)
	}
}

// ResetSync is synchronous version of Reset
func ResetSync(url, migrationsPath string) (err []error, ok bool) {
	pipe := pipep.New()
	go Reset(pipe, pipep.Signal(), url, migrationsPath)
	err = pipep.ReadErrors(pipe)
	return err, len(err) == 0
}

// Migrate applies relative +n/-n migrations
func Migrate(pipe chan interface{}, signal chan os.Signal, url, migrationsPath string, relativeN int) {
	d, files, version, err := initDriverAndReadMigrationFilesAndGetVersion(url, migrationsPath)
	if err != nil {
		go pipep.Close(pipe, err)
		return
	}

	applyMigrationFiles, err := files.From(version, relativeN)
	if err != nil {
		go pipep.Close(pipe, err)
		return
	}

	if len(applyMigrationFiles) > 0 && relativeN != 0 {
		for _, f := range applyMigrationFiles {
			pipe1 := pipep.New()
			go d.Migrate(f, pipe1)
			if ok := pipep.WaitAndRedirect(pipe1, pipe, signal); !ok {
				break
			}
		}
		go pipep.Close(pipe, nil)
		return
	}
	go pipep.Close(pipe, nil)
	return
}

// MigrateSync is synchronous version of Migrate
func MigrateSync(url, migrationsPath string, relativeN int) (err []error, ok bool) {
	pipe := pipep.New()
	go Migrate(pipe, pipep.Signal(), url, migrationsPath, relativeN)
	err = pipep.ReadErrors(pipe)
	return err, len(err) == 0
}

// Version returns the current migration version
func Version(url, migrationsPath string) (version uint64, err error) {
	d, err := driver.New(url)
	if err != nil {
		return 0, err
	}
	return d.Version()
}

// Create creates new migration files on disk
func Create(url, migrationsPath, name string) (*file.MigrationFile, error) {
	d, err := driver.New(url)
	if err != nil {
		return nil, err
	}
	files, err := file.ReadMigrationFiles(migrationsPath, file.FilenameRegex(d.FilenameExtension()))
	if err != nil {
		return nil, err
	}

	version := uint64(0)
	if len(files) > 0 {
		lastFile := files[len(files)-1]
		version = lastFile.Version
	}
	version += 1
	versionStr := strconv.FormatUint(version, 10)

	length := 4 // TODO(mattes) check existing files and try to guess length
	if len(versionStr)%length != 0 {
		versionStr = strings.Repeat("0", length-len(versionStr)%length) + versionStr
	}

	filenamef := "%s_%s.%s.%s"
	name = strings.Replace(name, " ", "_", -1)

	mfile := &file.MigrationFile{
		Version: version,
		UpFile: &file.File{
			Path:      migrationsPath,
			FileName:  fmt.Sprintf(filenamef, versionStr, name, "up", d.FilenameExtension()),
			Name:      name,
			Content:   []byte(""),
			Direction: direction.Up,
		},
		DownFile: &file.File{
			Path:      migrationsPath,
			FileName:  fmt.Sprintf(filenamef, versionStr, name, "down", d.FilenameExtension()),
			Name:      name,
			Content:   []byte(""),
			Direction: direction.Down,
		},
	}

	if err := ioutil.WriteFile(path.Join(mfile.UpFile.Path, mfile.UpFile.FileName), mfile.UpFile.Content, 0644); err != nil {
		return nil, err
	}
	if err := ioutil.WriteFile(path.Join(mfile.DownFile.Path, mfile.DownFile.FileName), mfile.DownFile.Content, 0644); err != nil {
		return nil, err
	}

	return mfile, nil
}

// initDriverAndReadMigrationFilesAndGetVersion is a small helper
// function that is common to most of the migration funcs
func initDriverAndReadMigrationFilesAndGetVersion(url, migrationsPath string) (driver.Driver, *file.MigrationFiles, uint64, error) {
	d, err := driver.New(url)
	if err != nil {
		return nil, nil, 0, err
	}
	files, err := file.ReadMigrationFiles(migrationsPath, file.FilenameRegex(d.FilenameExtension()))
	if err != nil {
		return nil, nil, 0, err
	}
	version, err := d.Version()
	if err != nil {
		return nil, nil, 0, err
	}
	return d, &files, version, nil
}

// NewPipe is a convenience function for pipe.New().
// This is helpful if the user just wants to import this package and nothing else.
func NewPipe() chan interface{} {
	return pipep.New()
}
