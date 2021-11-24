//go:build sqlite
// +build sqlite

package cli

import (
	_ "github.com/golang-migrate/migrate/v4/database/sqlite"
)
