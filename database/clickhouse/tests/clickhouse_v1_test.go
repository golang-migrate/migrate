//go:build clickhouse_v1_driver_test
// +build clickhouse_v1_driver_test

package tests

import (
	"testing"

	_ "github.com/ClickHouse/clickhouse-go" // Register v1 driver for this test
)

func TestV1DriverCompatibility(t *testing.T) {
	runSharedTestCases(t)
}
