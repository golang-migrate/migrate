//go:build duckdb
// +build duckdb

package cli

import (
	_ "github.com/golang-migrate/migrate/v4/database/duckdb"
)
