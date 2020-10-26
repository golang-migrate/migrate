package mem

import (
	"errors"
	"fmt"
)

var (
	ErrEmptyKey         = errors.New("key is empty")
	ErrNilMigration     = errors.New("some migration(s) is nil")
	ErrInvalidUrlScheme = errors.New("url scheme must be mem://")
)

// ErrDuplicateVersion error type when duplicate version occurred
type ErrDuplicateVersion struct {
	key string
	ver uint
}

// Error implements Go error interface
func (e ErrDuplicateVersion) Error() string {
	return fmt.Sprintf("duplicate version %d for existing key '%s'", e.ver, e.key)
}
