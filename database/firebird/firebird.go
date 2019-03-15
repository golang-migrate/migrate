// +build go1.9

package firebird

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
	"github.com/hashicorp/go-multierror"
	_ "github.com/nakagami/firebirdsql"
	"io"
	"io/ioutil"
	nurl "net/url"
	"strings"
)

func init() {
	db := Firebird{}
	database.Register("firebird", &db)
	database.Register("firebirdsql", &db)
}

var DefaultMigrationsTable = "SCHEMA_MIGRATIONS"

var (
	ErrNilConfig = fmt.Errorf("no config")
)

type Config struct {
	DatabaseName    string
	MigrationsTable string
	SchemaName      string
}

type Firebird struct {
	// Locking and unlocking need to use the same connection
	conn     *sql.Conn
	db       *sql.DB
	isLocked bool

	// Open and WithInstance need to garantuee that config is never nil
	config *Config
}

func WithInstance(instance *sql.DB, config *Config) (database.Driver, error) {
	if config == nil {
		return nil, ErrNilConfig
	}

	if err := instance.Ping(); err != nil {
		return nil, err
	}

	if len(config.MigrationsTable) == 0 {
		config.MigrationsTable = DefaultMigrationsTable
	}

	conn, err := instance.Conn(context.Background())
	if err != nil {
		return nil, err
	}

	fb := &Firebird{
		conn:   conn,
		db:     instance,
		config: config,
	}

	config.DatabaseName = strings.ToUpper(config.DatabaseName)

	if err := fb.ensureVersionTable(); err != nil {
		return nil, err
	}

	return fb, nil
}

func (f *Firebird) Open(dsn string) (database.Driver, error) {
	purl, err := nurl.Parse(dsn)
	if err != nil {
		return nil, err
	}

	db, err := sql.Open("firebirdsql", migrate.FilterCustomQuery(purl).String())
	if err != nil {
		return nil, err
	}

	px, err := WithInstance(db, &Config{
		MigrationsTable: purl.Query().Get("x-migrations-table"),
		DatabaseName:    purl.Path,
	})

	if err != nil {
		return nil, err
	}

	return px, nil
}

func (f *Firebird) Close() error {
	connErr := f.conn.Close()
	dbErr := f.db.Close()
	if connErr != nil || dbErr != nil {
		return fmt.Errorf("conn: %v, db: %v", connErr, dbErr)
	}
	return nil
}

func (f *Firebird) Lock() error {
	if f.isLocked {
		return database.ErrLocked
	}
	f.isLocked = true
	return nil
}

func (f *Firebird) Unlock() error {
	f.isLocked = false
	return nil
}

func (f *Firebird) Run(migration io.Reader) error {
	migr, err := ioutil.ReadAll(migration)
	if err != nil {
		return err
	}

	// run migration
	query := string(migr[:])
	if _, err := f.conn.ExecContext(context.Background(), query); err != nil {
		return database.Error{OrigErr: err, Err: "migration failed", Query: migr}
	}

	return nil
}

func (f *Firebird) SetVersion(version int, dirty bool) error {
	tx, err := f.conn.BeginTx(context.Background(), &sql.TxOptions{})
	if err != nil {
		return &database.Error{OrigErr: err, Err: "transaction start failed"}
	}

	query := `DELETE FROM "` + f.config.MigrationsTable + `"`
	if _, err := tx.Exec(query); err != nil {
		tx.Rollback()
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}

	if version >= 0 {
		query = `INSERT INTO "` + f.config.MigrationsTable + `" (version, dirty) VALUES (?, ?)`
		if _, err := tx.Exec(query, version, dirty); err != nil {
			tx.Rollback()
			return &database.Error{OrigErr: err, Query: []byte(query)}
		}
	}

	if err := tx.Commit(); err != nil {
		return &database.Error{OrigErr: err, Err: "transaction commit failed"}
	}

	return nil
}

func (f *Firebird) Version() (version int, dirty bool, err error) {
	query := `SELECT FIRST 1 version, dirty FROM "` + f.config.MigrationsTable + `"`
	err = f.conn.QueryRowContext(context.Background(), query).Scan(&version, &dirty)
	switch {
	case err == sql.ErrNoRows:
		return database.NilVersion, false, nil
	case err != nil:
		return 0, false, &database.Error{OrigErr: err, Query: []byte(query)}

	default:
		return version, dirty, nil
	}
}

func (f *Firebird) Drop() error {
	// select all tables
	query := `SELECT rdb$relation_name FROM rdb$relations WHERE rdb$view_blr IS NULL AND (rdb$system_flag IS NULL OR rdb$system_flag = 0);`
	tables, err := f.conn.QueryContext(context.Background(), query)
	if err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}
	defer tables.Close()

	// delete one table after another
	tableNames := make([]string, 0)
	for tables.Next() {
		var tableName string
		if err := tables.Scan(&tableName); err != nil {
			return err
		}
		if len(tableName) > 0 {
			tableNames = append(tableNames, tableName)
		}
	}

	if len(tableNames) > 0 {
		// delete one by one ...
		for _, t := range tableNames {
			query := `EXECUTE BLOCK AS BEGIN
							if (not exists(select 1 from rdb$relations where rdb$relation_name = '` + t + `')) then
							execute statement 'drop table "` + t + `"';
					  END;`
			if _, err := f.conn.ExecContext(context.Background(), query); err != nil {
				return &database.Error{OrigErr: err, Query: []byte(query)}
			}
		}
	}

	return nil
}

// ensureVersionTable checks if versions table exists and, if not, creates it.
func (f *Firebird) ensureVersionTable() (err error) {
	if err = f.Lock(); err != nil {
		return err
	}

	defer func() {
		if e := f.Unlock(); e != nil {
			if err == nil {
				err = e
			} else {
				err = multierror.Append(err, e)
			}
		}
	}()

	query := `EXECUTE BLOCK AS BEGIN
				if (not exists(select 1 from rdb$relations where rdb$relation_name = '` + f.config.MigrationsTable + `')) then
				execute statement 'create table "` + f.config.MigrationsTable + `" (version bigint not null primary key, dirty boolean not null)';
			  END;`
	if _, err = f.conn.ExecContext(context.Background(), query); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}

	return nil
}
