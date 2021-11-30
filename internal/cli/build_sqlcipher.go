//go:build sqlcipher
// +build sqlcipher

package cli

import (
	_ "github.com/golang-migrate/migrate/v4/database/sqlcipher"
)
