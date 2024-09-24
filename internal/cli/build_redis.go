//go:build redis
// +build redis

package cli

import (
	_ "github.com/golang-migrate/migrate/v4/database/redis"
)
