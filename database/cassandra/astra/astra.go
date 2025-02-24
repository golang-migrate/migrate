package cassandra

import (
	gocqlastra "github.com/datastax/gocql-astra"
	"github.com/golang-migrate/migrate/v4/database"
	"github.com/golang-migrate/migrate/v4/database/cassandra"
)

func init() {
	db := new(Astra)
	database.Register("astra", db)
}

type Astra = cassandra.Cassandra

func init() {
	cassandra.GocqlastraNewClusterFromURL = gocqlastra.NewClusterFromURL
	cassandra.GocqlastraNewClusterFromBundle = gocqlastra.NewClusterFromBundle
}
