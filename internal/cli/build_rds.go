//go:build rds
// +build rds

package cli

import (
	_ "github.com/golang-migrate/migrate/v4/database/rds"
)
