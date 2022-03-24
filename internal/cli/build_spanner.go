//go:build spanner
// +build spanner

package cli

import (
	_ "github.com/golang-migrate/migrate/v4/database/spanner"
)
