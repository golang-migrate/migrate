//go:build rdsmysql
// +build rdsmysql

package cli

import (
	_ "github.com/golang-migrate/migrate/v4/database/rdsmysql"
)
