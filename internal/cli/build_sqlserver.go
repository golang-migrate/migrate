//go:build sqlserver
// +build sqlserver

package cli

import (
	_ "github.com/golang-migrate/migrate/v4/database/sqlserver"
)
