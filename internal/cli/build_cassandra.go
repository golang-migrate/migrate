//go:build cassandra
// +build cassandra

package cli

import (
	_ "github.com/golang-migrate/migrate/v4/database/cassandra"
)
