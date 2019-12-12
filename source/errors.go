package source

// ErrDuplicateMigration is an error type for reporting duplicate migration
// files.
type ErrDuplicateMigration struct {
	Filename string
}

// Error implements error interface.
func (e ErrDuplicateMigration) Error() string {
	return "duplicate migration file: " + e.Filename
}
