//go:build ydb

package cli

import (
	_ "github.com/golang-migrate/migrate/v4/database/ydb"
	_ "github.com/ydb-platform/ydb-go-sdk/v3"
)
