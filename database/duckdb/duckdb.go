package duckdb

import (
	"database/sql"
	"fmt"
	"io"
	nurl "net/url"
	"strings"

	"go.uber.org/atomic"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
	"github.com/hashicorp/go-multierror"
	_ "github.com/marcboeker/go-duckdb"
)

func init() {
	database.Register("duckdb", &DuckDB{})
}

const MigrationTable = "gmg_schema_migrations"

type DuckDB struct {
	db       *sql.DB
	isLocked atomic.Bool
}

func (d *DuckDB) Open(url string) (database.Driver, error) {
	purl, err := nurl.Parse(url)
	if err != nil {
		return nil, fmt.Errorf("parsing url: %w", err)
	}
	dbfile := strings.Replace(migrate.FilterCustomQuery(purl).String(), "duckdb://", "", 1)
	db, err := sql.Open("duckdb", dbfile)
	if err != nil {
		return nil, fmt.Errorf("opening '%s': %w", dbfile, err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("pinging: %w", err)
	}
	d.db = db

	if err := d.ensureVersionTable(); err != nil {
		return nil, fmt.Errorf("ensuring version table: %w", err)
	}

	return d, nil
}

func (d *DuckDB) Close() error {
	return d.db.Close()
}

func (d *DuckDB) Lock() error {
	if !d.isLocked.CAS(false, true) {
		return database.ErrLocked
	}
	return nil
}

func (d *DuckDB) Unlock() error {
	if !d.isLocked.CAS(true, false) {
		return database.ErrNotLocked
	}
	return nil
}

func (d *DuckDB) Drop() error {
	tablesQuery := `SELECT schema_name, table_name FROM duckdb_tables()`
	tables, err := d.db.Query(tablesQuery)
	if err != nil {
		return &database.Error{OrigErr: err, Query: []byte(tablesQuery)}
	}
	defer func() {
		if errClose := tables.Close(); errClose != nil {
			err = multierror.Append(err, errClose)
		}
	}()

	tableNames := []string{}
	for tables.Next() {
		var (
			schemaName string
			tableName  string
		)

		if err := tables.Scan(&schemaName, &tableName); err != nil {
			return &database.Error{OrigErr: err, Err: "scanning schema and table name"}
		}

		if len(schemaName) > 0 {
			tableNames = append(tableNames, fmt.Sprintf("%s.%s", schemaName, tableName))
		} else {
			tableNames = append(tableNames, tableName)
		}
	}
	if err := tables.Err(); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(tablesQuery), Err: "err in rows after scanning"}
	}

	for _, t := range tableNames {
		dropQuery := fmt.Sprintf("DROP TABLE %s", t)
		if _, err := d.db.Exec(dropQuery); err != nil {
			return &database.Error{OrigErr: err, Query: []byte(dropQuery)}
		}
	}

	return nil

}

func (d *DuckDB) SetVersion(version int, dirty bool) error {
	tx, err := d.db.Begin()
	if err != nil {
		return &database.Error{OrigErr: err, Err: "transaction start failed"}
	}

	query := "DELETE FROM " + MigrationTable
	if _, err := tx.Exec(query); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}

	// Also re-write the schema version for nil dirty versions to prevent
	// empty schema version for failed down migration on the first migration
	// See: https://github.com/golang-migrate/migrate/issues/330
	//
	// NOTE: Copied from sqlite implementation, unsure if this is necessary for
	// duckdb
	if version >= 0 || (version == database.NilVersion && dirty) {
		query := fmt.Sprintf(`INSERT INTO %s (version, dirty) VALUES (?, ?)`, MigrationTable)
		if _, err := tx.Exec(query, version, dirty); err != nil {
			if errRollback := tx.Rollback(); errRollback != nil {
				err = multierror.Append(err, errRollback)
			}
			return &database.Error{OrigErr: err, Query: []byte(query)}
		}
	}

	if err := tx.Commit(); err != nil {
		return &database.Error{OrigErr: err, Err: "transaction commit failed"}
	}

	return nil
}

func (m *DuckDB) Version() (version int, dirty bool, err error) {
	query := "SELECT version, dirty FROM " + MigrationTable + " LIMIT 1"
	err = m.db.QueryRow(query).Scan(&version, &dirty)
	if err != nil {
		return database.NilVersion, false, nil
	}
	return version, dirty, nil
}

func (d *DuckDB) Run(migration io.Reader) error {
	migr, err := io.ReadAll(migration)
	if err != nil {
		return fmt.Errorf("reading migration: %w", err)
	}
	query := string(migr[:])

	tx, err := d.db.Begin()
	if err != nil {
		return &database.Error{OrigErr: err, Err: "transaction start failed"}
	}
	if _, err := tx.Exec(query); err != nil {
		if errRollback := tx.Rollback(); errRollback != nil {
			err = multierror.Append(err, errRollback)
		}
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}
	if err := tx.Commit(); err != nil {
		return &database.Error{OrigErr: err, Err: "transaction commit failed"}
	}
	return nil
}

// ensureVersionTable checks if versions table exists and, if not, creates it.
// Note that this function locks the database, which deviates from the usual
// convention of "caller locks" in the Sqlite type.
func (d *DuckDB) ensureVersionTable() (err error) {
	if err = d.Lock(); err != nil {
		return err
	}

	defer func() {
		if e := d.Unlock(); e != nil {
			if err == nil {
				err = e
			} else {
				err = multierror.Append(err, e)
			}
		}
	}()

	query := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (version BIGINT, dirty BOOLEAN);`, MigrationTable)

	if _, err := d.db.Exec(query); err != nil {
		return fmt.Errorf("creating version table via '%s': %w", query, err)
	}
	return nil
}
