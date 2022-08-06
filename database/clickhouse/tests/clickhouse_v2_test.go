//go:build !clickhouse_v1_driver_test
// +build !clickhouse_v1_driver_test

package tests

import (
	"testing"

	_ "github.com/ClickHouse/clickhouse-go/v2" // Register v2 driver for this test
)

func TestV2DriverCompatibility(t *testing.T) {
	runSharedTestCases(t)
}
