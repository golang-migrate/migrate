//go:build trino
// +build trino

package cli

import (
	_ "github.com/golang-migrate/migrate/v4/database/trino"
)