//go:build opensearch
// +build opensearch

package cli

import (
	_ "github.com/golang-migrate/migrate/v4/database/opensearch"
)
