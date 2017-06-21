package clickhouse

import (
	"database/sql"
	"io"
	"io/ioutil"
	"net/url"
	"time"

	"github.com/mattes/migrate"
	"github.com/mattes/migrate/database"
)

var DefaultMigrationsTable = "schema_migrations"

type config struct {
	table    string
	database string
}

func init() {
	database.Register("clickhouse", &ClickHouse{})
}

type ClickHouse struct {
	conn   *sql.DB
	config config
}

func (ch *ClickHouse) Open(dsn string) (database.Driver, error) {
	purl, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}
	q := migrate.FilterCustomQuery(purl)
	q.Scheme = "tcp"
	conn, err := sql.Open("clickhouse", q.String())
	if err != nil {
		return nil, err
	}
	table := purl.Query().Get("x-migrations-table")
	if len(table) == 0 {
		table = DefaultMigrationsTable
	}
	database := purl.Query().Get("database")
	if len(database) == 0 {
		if err := conn.QueryRow("SELECT currentDatabase()").Scan(&database); err != nil {
			return nil, err
		}
	}
	ch = &ClickHouse{
		conn: conn,
		config: config{
			table:    table,
			database: database,
		},
	}
	if err := ch.ensureVersionTable(); err != nil {
		return nil, err
	}
	return ch, nil
}

func (ch *ClickHouse) Run(r io.Reader) error {
	migration, err := ioutil.ReadAll(r)
	if err != nil {
		return err
	}
	if _, err := ch.conn.Exec(string(migration)); err != nil {
		return database.Error{OrigErr: err, Err: "migration failed", Query: migration}
	}

	return nil
}
func (ch *ClickHouse) Version() (int, bool, error) {
	var (
		version int
		dirty   uint8
		query   = "SELECT version, dirty FROM `" + ch.config.table + "` ORDER BY sequence DESC LIMIT 1"
	)
	if err := ch.conn.QueryRow(query).Scan(&version, &dirty); err != nil {
		if err == sql.ErrNoRows {
			return database.NilVersion, false, nil
		}
		return 0, false, &database.Error{OrigErr: err, Query: []byte(query)}
	}
	return version, dirty == 1, nil
}

func (ch *ClickHouse) SetVersion(version int, dirty bool) error {
	var (
		bool = func(v bool) uint8 {
			if v {
				return 1
			}
			return 0
		}
		tx, err = ch.conn.Begin()
	)
	if err != nil {
		return err
	}
	query := "INSERT INTO " + ch.config.table + " (version, dirty, sequence) VALUES (?, ?, ?)"
	if _, err := tx.Exec(query, version, bool(dirty), time.Now().UnixNano()); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}
	return tx.Commit()
}

func (ch *ClickHouse) ensureVersionTable() error {
	var (
		table string
		query = "SHOW TABLES FROM " + ch.config.database + " LIKE '" + ch.config.table + "'"
	)
	// check if migration table exists
	if err := ch.conn.QueryRow(query).Scan(&table); err != nil {
		if err != sql.ErrNoRows {
			return &database.Error{OrigErr: err, Query: []byte(query)}
		}
	} else {
		return nil
	}
	// if not, create the empty migration table
	query = `
		CREATE TABLE ` + ch.config.table + ` (
			version    UInt32, 
			dirty      UInt8,
			sequence   UInt64
		) Engine=TinyLog
	`
	if _, err := ch.conn.Exec(query); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}
	return nil
}

func (ch *ClickHouse) Drop() error {
	var (
		query       = "SHOW TABLES FROM " + ch.config.database
		tables, err = ch.conn.Query(query)
	)
	if err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}
	defer tables.Close()
	for tables.Next() {
		var table string
		if err := tables.Scan(&table); err != nil {
			return err
		}
		query = "DROP TABLE IF EXISTS " + ch.config.database + "." + table
		if _, err := ch.conn.Exec(query); err != nil {
			return &database.Error{OrigErr: err, Query: []byte(query)}
		}
	}
	return ch.ensureVersionTable()
}

func (ch *ClickHouse) Lock() error   { return nil }
func (ch *ClickHouse) Unlock() error { return nil }
func (ch *ClickHouse) Close() error  { return ch.conn.Close() }
