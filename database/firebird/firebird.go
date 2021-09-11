//go:build go1.9
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
	"go.uber.org/atomic"
	"io"
	"io/ioutil"
	nurl "net/url"
)

func init() {
	db := Firebird{}
	database.Register("firebird", &db)
	database.Register("firebirdsql", &db)
}

var DefaultMigrationsTable = "schema_migrations"

var (
	ErrNilConfig = fmt.Errorf("no config")
)

type Config struct {
	DatabaseName    string
	MigrationsTable string
}

type Firebird struct {
	// Locking and unlocking need to use the same connection
	conn     *sql.Conn
	db       *sql.DB
	isLocked atomic.Bool

	// Open and WithInstance need to guarantee that config is never nil
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
	if !f.isLocked.CAS(false, true) {
		return database.ErrLocked
	}
	return nil
}

func (f *Firebird) Unlock() error {
	if !f.isLocked.CAS(true, false) {
		return database.ErrNotLocked
	}
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
	// Always re-write the schema version to prevent empty schema version
	// for failed down migration on the first migration
	// See: https://github.com/golang-migrate/migrate/issues/330

	// TODO: parameterize this SQL statement
	//       https://firebirdsql.org/refdocs/langrefupd20-execblock.html
	//       VALUES (?, ?) doesn't work
	query := fmt.Sprintf(`EXECUTE BLOCK AS BEGIN
					DELETE FROM "%v";
					INSERT INTO "%v" (version, dirty) VALUES (%v, %v);
				END;`,
		f.config.MigrationsTable, f.config.MigrationsTable, version, btoi(dirty))

	if _, err := f.conn.ExecContext(context.Background(), query); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}

	return nil
}

func (f *Firebird) Version() (version int, dirty bool, err error) {
	var d int
	query := fmt.Sprintf(`SELECT FIRST 1 version, dirty FROM "%v"`, f.config.MigrationsTable)
	err = f.conn.QueryRowContext(context.Background(), query).Scan(&version, &d)
	switch {
	case err == sql.ErrNoRows:
		return database.NilVersion, false, nil
	case err != nil:
		return 0, false, &database.Error{OrigErr: err, Query: []byte(query)}

	default:
		return version, itob(d), nil
	}
}

func (f *Firebird) Drop() (err error) {
	// select all tables
	query := `SELECT rdb$relation_name FROM rdb$relations WHERE rdb$view_blr IS NULL AND (rdb$system_flag IS NULL OR rdb$system_flag = 0);`
	tables, err := f.conn.QueryContext(context.Background(), query)
	if err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}
	defer func() {
		if errClose := tables.Close(); errClose != nil {
			err = multierror.Append(err, errClose)
		}
	}()

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
	if err := tables.Err(); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}

	// delete one by one ...
	for _, t := range tableNames {
		query := fmt.Sprintf(`EXECUTE BLOCK AS BEGIN
						if (not exists(select 1 from rdb$relations where rdb$relation_name = '%v')) then
						execute statement 'drop table "%v"';
					END;`,
			t, t)

		if _, err := f.conn.ExecContext(context.Background(), query); err != nil {
			return &database.Error{OrigErr: err, Query: []byte(query)}
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

	query := fmt.Sprintf(`EXECUTE BLOCK AS BEGIN
			if (not exists(select 1 from rdb$relations where rdb$relation_name = '%v')) then
			execute statement 'create table "%v" (version bigint not null primary key, dirty smallint not null)';
		END;`,
		f.config.MigrationsTable, f.config.MigrationsTable)

	if _, err = f.conn.ExecContext(context.Background(), query); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}

	return nil
}

// btoi converts bool to int
func btoi(v bool) int {
	if v {
		return 1
	}
	return 0
}

// itob converts int to bool
func itob(v int) bool {
	return v != 0
}
