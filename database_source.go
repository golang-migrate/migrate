package migrate

import (
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/golang-migrate/migrate/v4/database"
	"github.com/golang-migrate/migrate/v4/source"
)

// DatabaseSource implements source.Driver by reading migrations from database storage
type DatabaseSource struct {
	storageDriver database.MigrationStorageDriver
	logger        Logger
	versions      []uint
}

var _ source.Driver = &DatabaseSource{}

// Open is not used for DatabaseSource as it's created directly
func (d *DatabaseSource) Open(url string) (source.Driver, error) {
	return d, nil
}

// Close closes the database source
func (d *DatabaseSource) Close() error {
	return nil
}

// First returns the first migration version available in the database
func (d *DatabaseSource) First() (version uint, err error) {
	if err := d.loadVersions(); err != nil {
		return 0, err
	}
	
	if len(d.versions) == 0 {
		return 0, os.ErrNotExist
	}
	
	return d.versions[0], nil
}

// Prev returns the previous migration version relative to the current version
func (d *DatabaseSource) Prev(version uint) (prevVersion uint, err error) {
	if err := d.loadVersions(); err != nil {
		return 0, err
	}
	
	for i, v := range d.versions {
		if v == version && i > 0 {
			return d.versions[i-1], nil
		}
	}
	
	return 0, os.ErrNotExist
}

// Next returns the next migration version relative to the current version
func (d *DatabaseSource) Next(version uint) (nextVersion uint, err error) {
	if err := d.loadVersions(); err != nil {
		return 0, err
	}
	
	for i, v := range d.versions {
		if v == version && i < len(d.versions)-1 {
			return d.versions[i+1], nil
		}
	}
	
	return 0, os.ErrNotExist
}

// ReadUp reads the up migration for the specified version from the database
func (d *DatabaseSource) ReadUp(version uint) (r io.ReadCloser, identifier string, err error) {
	upScript, _, err := d.storageDriver.GetMigration(version)
	if err != nil {
		return nil, "", err
	}
	
	if len(upScript) == 0 {
		return nil, "", os.ErrNotExist
	}
	
	reader := io.NopCloser(strings.NewReader(string(upScript)))
	identifier = fmt.Sprintf("%d.up", version)
	
	return reader, identifier, nil
}

// ReadDown reads the down migration for the specified version from the database
func (d *DatabaseSource) ReadDown(version uint) (r io.ReadCloser, identifier string, err error) {
	_, downScript, err := d.storageDriver.GetMigration(version)
	if err != nil {
		return nil, "", err
	}
	
	if len(downScript) == 0 {
		return nil, "", os.ErrNotExist
	}
	
	reader := io.NopCloser(strings.NewReader(string(downScript)))
	identifier = fmt.Sprintf("%d.down", version)
	
	return reader, identifier, nil
}

// loadVersions loads available migration versions from the database
func (d *DatabaseSource) loadVersions() error {
	if d.versions != nil {
		return nil // Already loaded
	}
	
	versions, err := d.storageDriver.GetStoredMigrations()
	if err != nil {
		return fmt.Errorf("failed to load migrations from database: %w", err)
	}
	
	d.versions = versions
	return nil
}

// logPrintf writes to the logger if available
func (d *DatabaseSource) logPrintf(format string, v ...interface{}) {
	if d.logger != nil {
		d.logger.Printf(format, v...)
	}
}
