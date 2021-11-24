//go:build mongodb
// +build mongodb

package cli

import (
	_ "github.com/golang-migrate/migrate/v4/database/mongodb"
)
