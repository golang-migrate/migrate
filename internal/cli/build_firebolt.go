//go:build firebolt

package cli

import (
	_ "github.com/firebolt-db/firebolt-go-sdk"
	_ "github.com/golang-migrate/migrate/v4/database/firebolt"
)
