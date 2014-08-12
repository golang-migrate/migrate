package migrate

import (
	"errors"
	"fmt"
	"github.com/mattes/migrate/driver"
	"github.com/mattes/migrate/file"
	"github.com/mattes/migrate/migrate/direction"
	pipep "github.com/mattes/migrate/pipe"
	"io/ioutil"
	"path"
	"strconv"
	"strings"
)

// initDriverAndReadMigrationFilesAndGetVersion is a small helper
// function that is common to most of the Up and Down funcs
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

func Up(pipe chan interface{}, url, migrationsPath string) {
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
		go d.Migrate(applyMigrationFiles, pipe)
		return
	} else {
		go pipep.Close(pipe, errors.New("No migration files to apply."))
		return
	}
}

func UpSync(url, migrationsPath string) (err []error, ok bool) {
	pipe := pipep.New()
	go Up(pipe, url, migrationsPath)
	err = pipep.ReadErrors(pipe)
	return err, len(err) == 0
}

func Down(pipe chan interface{}, url, migrationsPath string) {
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
		go d.Migrate(applyMigrationFiles, pipe)
		return
	} else {
		go pipep.Close(pipe, errors.New("No migration files to apply."))
		return
	}
}

func DownSync(url, migrationsPath string) (err []error, ok bool) {
	pipe := pipep.New()
	go Down(pipe, url, migrationsPath)
	err = pipep.ReadErrors(pipe)
	return err, len(err) == 0
}

func Redo(pipe chan interface{}, url, migrationsPath string) {
	pipe1 := pipep.New()
	go Migrate(pipe1, url, migrationsPath, -1)
	go pipep.Redirect(pipe1, pipe)
	pipep.Wait(pipe1)
	go Migrate(pipe, url, migrationsPath, +1)
}

func RedoSync(url, migrationsPath string) (err []error, ok bool) {
	pipe := pipep.New()
	go Redo(pipe, url, migrationsPath)
	err = pipep.ReadErrors(pipe)
	return err, len(err) == 0
}

func Reset(pipe chan interface{}, url, migrationsPath string) {
	pipe1 := pipep.New()
	go Down(pipe1, url, migrationsPath)
	go pipep.Redirect(pipe1, pipe)
	pipep.Wait(pipe1)
	go Up(pipe, url, migrationsPath)
}

func ResetSync(url, migrationsPath string) (err []error, ok bool) {
	pipe := pipep.New()
	go Reset(pipe, url, migrationsPath)
	err = pipep.ReadErrors(pipe)
	return err, len(err) == 0
}

func Migrate(pipe chan interface{}, url, migrationsPath string, relativeN int) {
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
	if len(applyMigrationFiles) > 0 {
		if relativeN > 0 {
			go d.Migrate(applyMigrationFiles, pipe)
			return
		} else if relativeN < 0 {
			go d.Migrate(applyMigrationFiles, pipe)
			return
		} else {
			go pipep.Close(pipe, errors.New("No migration files to apply."))
			return
		}
	}
	go pipep.Close(pipe, errors.New("No migration files to apply."))
	return
}

func MigrateSync(url, migrationsPath string, relativeN int) (err []error, ok bool) {
	pipe := pipep.New()
	go Migrate(pipe, url, migrationsPath, relativeN)
	err = pipep.ReadErrors(pipe)
	return err, len(err) == 0
}

func Version(url, migrationsPath string) (version uint64, err error) {
	d, err := driver.New(url)
	if err != nil {
		return 0, err
	}
	return d.Version()
}

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

	length := 4
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
