//go:build neo4j
// +build neo4j

package cli

import (
	_ "github.com/golang-migrate/migrate/v4/database/neo4j"
)
