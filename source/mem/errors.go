package mem

import (
	"errors"
	"fmt"
)

var (
	ErrEmptyKey         = errors.New("key is empty")
	ErrNilMigration     = errors.New("some migration(s) is nil")
	ErrEmptyUrl         = errors.New("url is empty")
	ErrInvalidUrlScheme = errors.New("url scheme must be inmem://")
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
