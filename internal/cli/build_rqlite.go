//go:build rqlite
// +build rqlite

package cli

import (
	_ "github.com/golang-migrate/migrate/v4/database/rqlite"
)
