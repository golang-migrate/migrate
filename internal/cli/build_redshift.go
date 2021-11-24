//go:build redshift
// +build redshift

package cli

import (
	_ "github.com/golang-migrate/migrate/v4/database/redshift"
)
