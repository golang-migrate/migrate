// +build clickhouse

package main

import (
	_ "github.com/golang-migrate/migrate/v3/database/clickhouse"
	_ "github.com/kshvakov/clickhouse"
)
