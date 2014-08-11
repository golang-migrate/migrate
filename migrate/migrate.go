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

func common(db, migrationsPath string) (driver.Driver, *file.MigrationFiles, uint64, error) {
	d, err := driver.New(db)
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

func Up(db, migrationsPath string) error {
	d, files, version, err := common(db, migrationsPath)
	if err != nil {
		return err
	}

	applyMigrationFiles, err := files.ToLastFrom(version)
	if err != nil {
		return err
	}
	if len(applyMigrationFiles) > 0 {
		return d.Migrate(applyMigrationFiles)
	}
	return errors.New("No migrations to apply.")
}

func Down(db, migrationsPath string) error {
	d, files, version, err := common(db, migrationsPath)
	if err != nil {
		return err
	}

	applyMigrationFiles, err := files.ToFirstFrom(version)
	if err != nil {
		return err
	}
	if len(applyMigrationFiles) > 0 {
		return d.Migrate(applyMigrationFiles)
	}
	return errors.New("No migrations to apply.")
}

func Redo(db, migrationsPath string) error {
	d, files, version, err := common(db, migrationsPath)
	if err != nil {
		return err
	}
	applyMigrationFilesDown, err := files.From(version, -1)
	if err != nil {
		return err
	}
	if len(applyMigrationFilesDown) > 0 {
		if err := d.Migrate(applyMigrationFilesDown); err != nil {
			return err
		}
	}
	applyMigrationFilesUp, err := files.From(version, +1)
	if err != nil {
		return err
	}
	if len(applyMigrationFilesUp) > 0 {
		return d.Migrate(applyMigrationFilesUp)
	}
	return errors.New("No migrations to apply.")
}

func Reset(db, migrationsPath string) error {
	d, files, version, err := common(db, migrationsPath)
	if err != nil {
		return err
	}
	applyMigrationFilesDown, err := files.ToFirstFrom(version)
	if err != nil {
		return err
	}
	if len(applyMigrationFilesDown) > 0 {
		if err := d.Migrate(applyMigrationFilesDown); err != nil {
			return err
		}
	}
	applyMigrationFilesUp, err := files.ToLastFrom(0)
	if err != nil {
		return err
	}
	if len(applyMigrationFilesUp) > 0 {
		return d.Migrate(applyMigrationFilesUp)
	}
	return errors.New("No migrations to apply.")
}

func Migrate(db, migrationsPath string, relativeN int) error {
	d, files, version, err := common(db, migrationsPath)
	if err != nil {
		return err
	}

	applyMigrationFiles, err := files.From(version, relativeN)
	if err != nil {
		return err
	}
	if len(applyMigrationFiles) > 0 {
		if relativeN > 0 {
			return d.Migrate(applyMigrationFiles)
		} else if relativeN < 0 {
			return d.Migrate(applyMigrationFiles)
		} else {
			return errors.New("No migrations to apply.")
		}
	}
	return errors.New("No migrations to apply.")
}

func Version(db, migrationsPath string) (version uint64, err error) {
	d, err := driver.New(db)
	if err != nil {
		return 0, err
	}
	return d.Version()
}

func Create(db, migrationsPath, name string) (*file.MigrationFile, error) {
	d, err := driver.New(db)
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
