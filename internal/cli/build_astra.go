//go:build astra
// +build astra

package cli

import (
	_ "github.com/golang-migrate/migrate/v4/database/cassandra/astra"
)
