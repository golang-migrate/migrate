//go:build databricks
// +build databricks

package cli

import (
	_ "github.com/golang-migrate/migrate/v4/database/databricks"
)
