package cassandra

import (
	"time"

	"github.com/gocql/gocql"
)

// These are stubs to keep from depending on the astra driver.
// The astra package assigns to these to unstub.
var (
	GocqlastraNewClusterFromURL = func(url string, databaseID string, token string, timeout time.Duration) (*gocql.ClusterConfig, error) {
		panic("should not be used for cassandra")
	}
	GocqlastraNewClusterFromBundle = func(path string, username string, password string, timeout time.Duration) (*gocql.ClusterConfig, error) {
		panic("should not be used for cassandra")
	}
)
