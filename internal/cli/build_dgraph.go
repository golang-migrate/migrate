//go:build dgraph
// +build dgraph

package cli

import (
	_ "github.com/golang-migrate/migrate/v4/database/dgraph"
)
