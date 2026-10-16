package pgx

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"
)

// pingOnlyDriver lets tests build a *sql.DB that answers Ping without a real
// database, which is all WithInstance needs when DatabaseName and SchemaName
// are set in the config.
type pingOnlyDriver struct{}

func (pingOnlyDriver) Open(string) (driver.Conn, error) { return pingOnlyConn{}, nil }

type pingOnlyConn struct{}

func (pingOnlyConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("prepare not supported")
}

func (pingOnlyConn) Close() error { return nil }

func (pingOnlyConn) Begin() (driver.Tx, error) {
	return nil, errors.New("transactions not supported")
}

func (pingOnlyConn) Ping(context.Context) error { return nil }

var _ driver.Pinger = pingOnlyConn{}

func init() { sql.Register("migrate-fake-pgx", pingOnlyDriver{}) }

// TestWithInstanceQuotedTableWithoutQuotes ensures a quoted-table config whose
// table name contains no quotes returns an error instead of panicking.
func TestWithInstanceQuotedTableWithoutQuotes(t *testing.T) {
	db, err := sql.Open("migrate-fake-pgx", "")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	_, err = WithInstance(db, &Config{
		DatabaseName:          "testdb",
		SchemaName:            "public",
		MigrationsTable:       "schema_migrations",
		MigrationsTableQuoted: true,
	})
	if err == nil {
		t.Fatal("expected an error when MigrationsTableQuoted is set without a quoted table name")
	}
}
