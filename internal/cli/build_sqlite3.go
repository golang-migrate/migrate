//go:build sqlite3
// +build sqlite3

package cli

import (
	_ "github.com/golang-migrate/migrate/v4/database/sqlite3"
)
