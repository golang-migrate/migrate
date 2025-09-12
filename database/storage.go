package database

// MigrationStorageDriver extends the basic Driver interface to support
// storing and retrieving migration scripts in the database itself.
// This is useful for dirty state handling when shared storage isn't available.
type MigrationStorageDriver interface {
	Driver

	// StoreMigration stores the up and down migration scripts for a given version
	// in the database. This allows for dirty state recovery without external files.
	StoreMigration(version uint, upScript, downScript []byte) error

	// GetMigration retrieves the stored migration scripts for a given version.
	// Returns the up and down scripts, or an error if the version doesn't exist.
	GetMigration(version uint) (upScript, downScript []byte, err error)

	// GetStoredMigrations returns all migration versions that have scripts stored
	// in the database, sorted in ascending order.
	GetStoredMigrations() ([]uint, error)

	// SyncMigrations ensures all available migrations up to maxVersion are stored
	// in the database. This should be called during migration runs to keep
	// the database in sync with available migration files.
	SyncMigrations(sourceDriver interface{}, maxVersion uint) error
}

// SupportsStorage checks if a driver supports migration script storage
func SupportsStorage(driver Driver) bool {
	_, ok := driver.(MigrationStorageDriver)
	return ok
}
