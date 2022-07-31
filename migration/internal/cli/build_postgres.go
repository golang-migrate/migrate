//go:build postgres
// +build postgres

package cli

import (
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
)
