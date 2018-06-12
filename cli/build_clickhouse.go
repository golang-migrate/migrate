// +build clickhouse

package main

import (
	_ "github.com/kshvakov/clickhouse"
	_ "github.com/basekit/migrate/database/clickhouse"
)
