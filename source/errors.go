package source

// ErrDuplicateMigration is an error type for reporting duplicate migration
// files.
type ErrDuplicateMigration struct {
	Migration
	FileInfo
}

// Error implements error interface.
func (e ErrDuplicateMigration) Error() string {
	return "duplicate migration file: " + e.Name()
}

// FileInfo is the interface that extracts the minimum required function from os.FileInfo by ErrDuplicateMigration.
type FileInfo interface {
	Name() string
}
