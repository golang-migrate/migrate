//go:build mysql
// +build mysql

package cli

import (
	_ "github.com/golang-migrate/migrate/v4/database/mysql"
)
