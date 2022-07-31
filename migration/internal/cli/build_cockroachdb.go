//go:build cockroachdb
// +build cockroachdb

package cli

import (
	_ "github.com/golang-migrate/migrate/v4/database/cockroachdb"
)
