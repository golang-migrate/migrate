// +build clickhouse

package cli

import (
	_ "github.com/golang-migrate/migrate/v4/database/clickhouse"
	_ "github.com/kshvakov/clickhouse"
)
