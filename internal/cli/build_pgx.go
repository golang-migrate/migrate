//go:build pgx
// +build pgx

package cli

import (
	_ "github.com/golang-migrate/migrate/v4/database/pgx"
)
