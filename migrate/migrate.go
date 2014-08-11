package migrate

import (
	"errors"
	"fmt"
	"github.com/mattes/migrate/driver"
	"github.com/mattes/migrate/file"
	"github.com/mattes/migrate/migrate/direction"
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

// getErrorsFromPipe selects all received errors and returns them
func getErrorsFromPipe(pipe chan interface{}) []error {
	err := make([]error, 0)
	if pipe != nil {
		for {
			select {
			case item, ok := <-pipe:
				if !ok {
					return err
				} else {
					switch item.(type) {
					case error:
						err = append(err, item.(error))
					}
				}
			}
		}
	}
	return err
}

// sendErrorAndClosePipe sends the error to the pipe and closes the
// pipe afterwards
func sendErrorAndClosePipe(err error, pipe chan interface{}) {
	pipe <- err
	close(pipe)
}

func UpPipe(url, migrationsPath string, pipe chan interface{}) chan interface{} {
	if pipe == nil {
		pipe = make(chan interface{}, 1) // make buffered channel with cap 1
	}

	d, files, version, err := initDriverAndReadMigrationFilesAndGetVersion(url, migrationsPath)
	if err != nil {
		sendErrorAndClosePipe(err, pipe)
		return pipe
	}

	applyMigrationFiles, err := files.ToLastFrom(version)
	if err != nil {
		sendErrorAndClosePipe(err, pipe)
		return pipe
	}

	if len(applyMigrationFiles) > 0 {
		go d.Migrate(applyMigrationFiles, pipe)
		return pipe
	} else {
		sendErrorAndClosePipe(errors.New("No migration files to apply."), pipe)
		return pipe
	}
}

func Up(url, migrationsPath string) (err []error, ok bool) {
	pipe := UpPipe(url, migrationsPath, nil)
	err = getErrorsFromPipe(pipe)
	return err, len(err) == 0
}

func DownPipe(url, migrationsPath string, pipe chan interface{}) chan interface{} {
	if pipe == nil {
		pipe = make(chan interface{}, 1) // make buffered channel with cap 1
	}

	d, files, version, err := initDriverAndReadMigrationFilesAndGetVersion(url, migrationsPath)
	if err != nil {
		sendErrorAndClosePipe(err, pipe)
		return pipe
	}

	applyMigrationFiles, err := files.ToFirstFrom(version)
	if err != nil {
		sendErrorAndClosePipe(err, pipe)
		return pipe
	}
	if len(applyMigrationFiles) > 0 {
		go d.Migrate(applyMigrationFiles, nil)
		return pipe
	} else {
		sendErrorAndClosePipe(errors.New("No migration files to apply."), pipe)
		return pipe
	}
}

func Down(url, migrationsPath string) (err []error, ok bool) {
	pipe := DownPipe(url, migrationsPath, nil)
	err = getErrorsFromPipe(pipe)
	return err, len(err) == 0
}

func RedoPipe(url, migrationsPath string, pipe chan interface{}) chan interface{} {
	if pipe == nil {
		pipe = make(chan interface{}, 1) // make buffered channel with cap 1
	}

	_ = MigratePipe(url, migrationsPath, -1, pipe)
	_ = MigratePipe(url, migrationsPath, +1, pipe)
	return pipe
}

func Redo(url, migrationsPath string) (err []error, ok bool) {
	pipe := RedoPipe(url, migrationsPath, nil)
	err = getErrorsFromPipe(pipe)
	return err, len(err) == 0
}

func ResetPipe(url, migrationsPath string, pipe chan interface{}) chan interface{} {
	if pipe == nil {
		pipe = make(chan interface{}, 1) // make buffered channel with cap 1
	}
	// TODO check pipe pointer
	_ = DownPipe(url, migrationsPath, pipe)
	_ = UpPipe(url, migrationsPath, pipe)
	return pipe
}

func Reset(url, migrationsPath string) (err []error, ok bool) {
	pipe := ResetPipe(url, migrationsPath, nil)
	err = getErrorsFromPipe(pipe)
	return err, len(err) == 0
}

func MigratePipe(url, migrationsPath string, relativeN int, pipe chan interface{}) chan interface{} {
	if pipe == nil {
		pipe = make(chan interface{}, 1) // make buffered channel with cap 1
	}

	d, files, version, err := initDriverAndReadMigrationFilesAndGetVersion(url, migrationsPath)
	if err != nil {
		sendErrorAndClosePipe(err, pipe)
		return pipe
	}

	applyMigrationFiles, err := files.From(version, relativeN)
	if err != nil {
		sendErrorAndClosePipe(err, pipe)
		return pipe
	}
	if len(applyMigrationFiles) > 0 {
		if relativeN > 0 {
			go d.Migrate(applyMigrationFiles, pipe)
			return pipe
		} else if relativeN < 0 {
			go d.Migrate(applyMigrationFiles, pipe)
			return pipe
		} else {
			sendErrorAndClosePipe(errors.New("No migration files to apply."), pipe)
			return pipe
		}
	}
	sendErrorAndClosePipe(errors.New("No migration files to apply."), pipe)
	return pipe
}

func Migrate(url, migrationsPath string, relativeN int) (err []error, ok bool) {
	pipe := MigratePipe(url, migrationsPath, relativeN, nil)
	err = getErrorsFromPipe(pipe)
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
