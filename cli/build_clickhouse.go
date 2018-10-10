// +build clickhouse

package main

import (
	_ "github.com/golang-migrate/migrate/database/clickhouse"
	_ "github.com/kshvakov/clickhouse"
)
