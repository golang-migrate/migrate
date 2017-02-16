package postgres

import (
	"database/sql"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	nurl "net/url"

	"github.com/lib/pq"
	"github.com/mattes/migrate/database"
)

func init() {
	database.Register("postgres", &Postgres{})
}

var DefaultMigrationsTable = "schema_migrations"

var (
	ErrNilConfig      = fmt.Errorf("no config")
	ErrNoDatabaseName = fmt.Errorf("no database name")
	ErrNoSchema       = fmt.Errorf("no schema")
	ErrDatabaseDirty  = fmt.Errorf("database is dirty")
)

type Config struct {
	MigrationsTable string
	DatabaseName    string
}

func WithInstance(instance *sql.DB, config *Config) (database.Driver, error) {
	if config == nil {
		return nil, ErrNilConfig
	}

	query := `SELECT CURRENT_DATABASE()`
	var databaseName string
	if err := instance.QueryRow(query).Scan(&databaseName); err != nil {
		return nil, &database.Error{OrigErr: err, Query: []byte(query)}
	}

	if len(databaseName) == 0 {
		return nil, ErrNoDatabaseName
	}

	config.DatabaseName = databaseName

	if len(config.MigrationsTable) == 0 {
		config.MigrationsTable = DefaultMigrationsTable
	}

	px := &Postgres{
		db:     instance,
		config: config,
	}

	if err := px.ensureVersionTable(); err != nil {
		return nil, err
	}

	return px, nil
}

type Postgres struct {
	db       *sql.DB
	isLocked bool

	// Open and WithInstance need to garantuee that config is never nil
	config *Config
}

func (p *Postgres) Open(url string) (database.Driver, error) {
	purl, err := nurl.Parse(url)
	if err != nil {
		return nil, err
	}

	db, err := sql.Open("postgres", url)
	if err != nil {
		return nil, err
	}

	migrationsTable := purl.Query().Get("x-migrations-table")
	if len(migrationsTable) == 0 {
		migrationsTable = DefaultMigrationsTable
	}

	px, err := WithInstance(db, &Config{
		DatabaseName:    purl.Path,
		MigrationsTable: migrationsTable,
	})
	if err != nil {
		return nil, err
	}

	if err := db.Ping(); err != nil {
		return nil, err
	}

	return px, nil
}

func (p *Postgres) Close() error {
	return p.db.Close()
}

// https://www.postgresql.org/docs/9.6/static/explicit-locking.html#ADVISORY-LOCKS
func (p *Postgres) Lock() error {
	if p.isLocked {
		return database.ErrLocked
	}

	aid, err := p.generateAdvisoryLockId()
	if err != nil {
		return err
	}

	// This will either obtain the lock immediately and return true,
	// or return false if the lock cannot be acquired immediately.
	query := `SELECT pg_try_advisory_lock($1)`
	var success bool
	if err := p.db.QueryRow(query, aid).Scan(&success); err != nil {
		return &database.Error{OrigErr: err, Err: "try lock failed", Query: []byte(query)}
	}

	if success {
		p.isLocked = true
		return nil
	}

	return database.ErrLocked
}

func (p *Postgres) Unlock() error {
	if !p.isLocked {
		return nil
	}

	aid, err := p.generateAdvisoryLockId()
	if err != nil {
		return err
	}

	query := `SELECT pg_advisory_unlock($1)`
	if _, err := p.db.Exec(query, aid); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}
	p.isLocked = false
	return nil
}

func (p *Postgres) Run(version int, migration io.Reader) error {
	if dirty, err := p.isDirty(); err != nil {
		return err
	} else if dirty {
		return ErrDatabaseDirty
	}

	if migration == nil {
		// just apply version
		return p.saveVersion(version, false)
	}

	migr, err := ioutil.ReadAll(migration)
	if err != nil {
		return err
	}

	// set dirty flag and set version
	if err := p.saveVersion(version, true); err != nil {
		return err
	}

	// run migration
	query := string(migr[:])
	if _, err := p.db.Exec(query); err != nil {
		// TODO: cast to postgress error and get line number
		return database.Error{OrigErr: err, Err: "migration failed", Query: migr}
	}

	// remove dirty flag
	return p.saveVersion(version, false)
}

func (p *Postgres) saveVersion(version int, dirty bool) error {
	tx, err := p.db.Begin()
	if err != nil {
		return &database.Error{OrigErr: err, Err: "transaction start failed"}
	}

	query := `TRUNCATE "` + p.config.MigrationsTable + `"`
	if _, err := p.db.Exec(query); err != nil {
		tx.Rollback()
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}

	if version >= 0 {
		query = `INSERT INTO "` + p.config.MigrationsTable + `" (version, dirty) VALUES ($1, $2)`
		if _, err := p.db.Exec(query, version, dirty); err != nil {
			tx.Rollback()
			return &database.Error{OrigErr: err, Query: []byte(query)}
		}
	}

	if err := tx.Commit(); err != nil {
		return &database.Error{OrigErr: err, Err: "transaction commit failed"}
	}

	return nil
}

func (p *Postgres) isDirty() (bool, error) {
	query := `SELECT dirty FROM "` + p.config.MigrationsTable + `" LIMIT 1`
	var dirty bool
	err := p.db.QueryRow(query).Scan(&dirty)
	switch {
	case err == sql.ErrNoRows:
		return false, nil

	case err != nil:
		if e, ok := err.(*pq.Error); ok {
			if e.Code.Name() == "undefined_table" {
				return false, nil
			}
		}
		return false, &database.Error{OrigErr: err, Query: []byte(query)}

	default:
		return dirty, nil
	}
}

func (p *Postgres) Version() (int, error) {
	query := `SELECT version FROM "` + p.config.MigrationsTable + `" LIMIT 1`
	var version uint64
	err := p.db.QueryRow(query).Scan(&version)
	switch {
	case err == sql.ErrNoRows:
		return database.NilVersion, nil

	case err != nil:
		if e, ok := err.(*pq.Error); ok {
			if e.Code.Name() == "undefined_table" {
				return database.NilVersion, nil
			}
		}
		return 0, &database.Error{OrigErr: err, Query: []byte(query)}

	default:
		return int(version), nil
	}
}

func (p *Postgres) Drop() error {
	// select all tables in current schema
	query := `SELECT table_name FROM information_schema.tables WHERE table_schema=(SELECT current_schema())`
	tables, err := p.db.Query(query)
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
			query = `DROP TABLE IF EXISTS ` + t + ` CASCADE`
			if _, err := p.db.Exec(query); err != nil {
				return &database.Error{OrigErr: err, Query: []byte(query)}
			}
		}
		if err := p.ensureVersionTable(); err != nil {
			return err
		}
	}

	return nil
}

func (p *Postgres) ensureVersionTable() error {
	// check if migration table exists
	var count int
	query := `SELECT COUNT(1) FROM information_schema.tables WHERE table_name = $1 AND table_schema = (SELECT current_schema()) LIMIT 1`
	if err := p.db.QueryRow(query, p.config.MigrationsTable).Scan(&count); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}
	if count == 1 {
		return nil
	}

	// if not, create the empty migration table
	query = `CREATE TABLE "` + p.config.MigrationsTable + `" (version bigint not null primary key, dirty boolean not null)`
	if _, err := p.db.Exec(query); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}
	return nil
}

const AdvisoryLockIdSalt uint = 1486364155

// inspired by rails migrations, see https://goo.gl/8o9bCT
func (p *Postgres) generateAdvisoryLockId() (string, error) {
	sum := crc32.ChecksumIEEE([]byte(p.config.DatabaseName))
	sum = sum * uint32(AdvisoryLockIdSalt)
	return fmt.Sprintf("%v", sum), nil
}
