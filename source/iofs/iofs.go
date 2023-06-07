//go:build go1.16
// +build go1.16

package iofs

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path"
	"path/filepath"
	"regexp"
	"strconv"

	"github.com/Masterminds/semver/v3"
	"github.com/sigurn/crc8"

	"github.com/golang-migrate/migrate/v4/source"
)

type driver struct {
	PartialDriver
}

// New returns a new Driver from io/fs#FS and a relative path.
func New(fsys fs.FS, path string) (source.Driver, error) {
	var i driver
	if err := i.Init(fsys, path); err != nil {
		return nil, fmt.Errorf("failed to init driver with path %s: %w", path, err)
	}
	return &i, nil
}

// Open is part of source.Driver interface implementation.
// Open cannot be called on the iofs passthrough driver.
func (d *driver) Open(url string) (source.Driver, error) {
	return nil, errors.New("Open() cannot be called on the iofs passthrough driver")
}

// PartialDriver is a helper service for creating new source drivers working with
// io/fs.FS instances. It implements all source.Driver interface methods
// except for Open(). New driver could embed this struct and add missing Open()
// method.
//
// To prepare PartialDriver for use Init() function.
type PartialDriver struct {
	migrations *source.Migrations
	fsys       fs.FS
	path       string
}

var semVerMigDirPat = regexp.MustCompile(
	`^v?(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-((?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?(?:\+([0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?$`,
)

// Init prepares not initialized IoFS instance to read migrations from a
// io/fs#FS instance and a relative path.
func (d *PartialDriver) Init(fsys fs.FS, path string) error {
	entries, err := fs.ReadDir(fsys, path)
	if err != nil {
		return err
	}
	type migrationFilePair struct {
		migration *source.Migration
		file      fs.FileInfo
	}

	var list []migrationFilePair
	ms := source.NewMigrations()
	for _, e := range entries {
		file, err := e.Info()
		if err != nil {
			return err
		}
		if !e.IsDir() {
			m, err := source.DefaultParse(e.Name())
			if err != nil {
				continue
			}
			list = append(list, migrationFilePair{migration: m, file: file})
		} else if e.IsDir() && semVerMigDirPat.MatchString(e.Name()) {
			mlist, err := ParseSemVer(fsys, path, e.Name())
			if err != nil {
				continue
			}
			for _, sm := range mlist {
				list = append(list, migrationFilePair{migration: sm, file: file})
			}
		}
	}

	for _, m := range list {
		if !ms.Append(m.migration) {
			return source.ErrDuplicateMigration{
				Migration: *m.migration,
				FileInfo:  m.file,
			}
		}
	}
	d.fsys = fsys
	d.path = path
	d.migrations = ms
	return nil
}

// Close is part of source.Driver interface implementation.
// Closes the file system if possible.
func (d *PartialDriver) Close() error {
	c, ok := d.fsys.(io.Closer)
	if !ok {
		return nil
	}
	return c.Close()
}

// First is part of source.Driver interface implementation.
func (d *PartialDriver) First() (version uint, err error) {
	if version, ok := d.migrations.First(); ok {
		return version, nil
	}
	return 0, &fs.PathError{
		Op:   "first",
		Path: d.path,
		Err:  fs.ErrNotExist,
	}
}

// Prev is part of source.Driver interface implementation.
func (d *PartialDriver) Prev(version uint) (prevVersion uint, err error) {
	if version, ok := d.migrations.Prev(version); ok {
		return version, nil
	}
	return 0, &fs.PathError{
		Op:   "prev for version " + strconv.FormatUint(uint64(version), 10),
		Path: d.path,
		Err:  fs.ErrNotExist,
	}
}

// Next is part of source.Driver interface implementation.
func (d *PartialDriver) Next(version uint) (nextVersion uint, err error) {
	if version, ok := d.migrations.Next(version); ok {
		return version, nil
	}
	return 0, &fs.PathError{
		Op:   "next for version " + strconv.FormatUint(uint64(version), 10),
		Path: d.path,
		Err:  fs.ErrNotExist,
	}
}

// ReadUp is part of source.Driver interface implementation.
func (d *PartialDriver) ReadUp(version uint) (r io.ReadCloser, identifier string, err error) {
	if m, ok := d.migrations.Up(version); ok {
		body, err := d.open(path.Join(d.path, m.Raw))
		if err != nil {
			return nil, "", err
		}
		return body, m.Identifier, nil
	}
	return nil, "", &fs.PathError{
		Op:   "read up for version " + strconv.FormatUint(uint64(version), 10),
		Path: d.path,
		Err:  fs.ErrNotExist,
	}
}

// ReadDown is part of source.Driver interface implementation.
func (d *PartialDriver) ReadDown(version uint) (r io.ReadCloser, identifier string, err error) {
	if m, ok := d.migrations.Down(version); ok {
		body, err := d.open(path.Join(d.path, m.Raw))
		if err != nil {
			return nil, "", err
		}
		return body, m.Identifier, nil
	}
	return nil, "", &fs.PathError{
		Op:   "read down for version " + strconv.FormatUint(uint64(version), 10),
		Path: d.path,
		Err:  fs.ErrNotExist,
	}
}

func (d *PartialDriver) open(path string) (fs.File, error) {
	f, err := d.fsys.Open(path)
	if err == nil {
		return f, nil
	}
	// Some non-standard file systems may return errors that don't include the path, that
	// makes debugging harder.
	if !errors.As(err, new(*fs.PathError)) {
		err = &fs.PathError{
			Op:   "open",
			Path: path,
			Err:  err,
		}
	}
	return nil, err
}

// ParseSemVer returns Migration for matching Regex pattern.
func ParseSemVer(fsys fs.FS, path string, semVerDir string) ([]*source.Migration, error) {
	versionUint64, err := toBigInt(semVerDir)
	if err != nil {
		return nil, err
	}
	entries, err := fs.ReadDir(fsys, filepath.Join(path, semVerDir))
	if err != nil {
		return nil, err
	}
	migs := make([]*source.Migration, 0, 64)
	for _, e := range entries {
		mig, err := source.ParseSemVer(versionUint64, semVerDir, e.Name())
		if err != nil {
			return nil, err
		}
		migs = append(migs, mig)
	}
	return migs, nil
}

func toBigInt(vstr string) (uint64, error) {
	var other string
	v, err := semver.NewVersion(vstr)
	if err != nil {
		return 0, err
	}
	other = v.Prerelease()
	var crc uint8
	if other != "" {
		table := crc8.MakeTable(crc8.CRC8_MAXIM)
		crc = crc8.Checksum([]byte(other), table)
	}

	ver := v.Major()*100*100*1000 + v.Minor()*100*1000 + v.Patch()*1000
	if crc == 0 {
		ver += 999
	} else {
		ver += uint64(crc)
	}
	return ver, nil
}
