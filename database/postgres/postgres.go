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

type Config struct {
}

func WithInstance(instance *sql.DB, config *Config) (database.Driver, error) {
	return &Postgres{
		db:     instance,
		config: config,
	}, nil
}

type Postgres struct {
	db       *sql.DB
	url      *nurl.URL
	isLocked bool
	config   *Config
}

var (
	ErrNoSqlInstance  = fmt.Errorf("expected *sql.DB")
	ErrNoDatabaseName = fmt.Errorf("no database name")
)

const tableName = "schema_migrations"

func (p *Postgres) Open(url string) (database.Driver, error) {
	purl, err := nurl.Parse(url)
	if err != nil {
		return nil, err
	}

	db, err := sql.Open("postgres", url)
	if err != nil {
		return nil, err
	}

	if err := db.Ping(); err != nil {
		return nil, err
	}

	px := &Postgres{
		db:  db,
		url: purl,
	}
	if err := px.ensureVersionTable(); err != nil {
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

	//  It will either obtain the lock immediately and return true, or return false if the lock cannot be acquired immediately.
	var success bool
	if err := p.db.QueryRow("SELECT pg_try_advisory_lock($1)", aid).Scan(&success); err != nil {
		return err
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

	if _, err := p.db.Exec("SELECT pg_advisory_unlock($1)", aid); err != nil {
		return err
	}
	p.isLocked = false
	return nil
}

func (p *Postgres) Run(version int, migration io.Reader) error {
	if migration == nil {
		// just apply version
		return p.saveVersion(version)
	}

	mgr, err := ioutil.ReadAll(migration)
	if err != nil {
		return err
	}

	// it would be nice to be able to wrap the migration into the transaction, too
	// unfortunately things like `CREATE INDEX CONCURRENTLY` aren't possible in a
	// transaction. so if something fails between running the migration, and
	// storing the latest migration version in the version table, we alert the user
	// who then needs to manually fix.
	// TODO: two phase commit?
	if _, err := p.db.Exec(string(mgr[:])); err != nil {
		return err
	}

	return p.saveVersion(version)
}

func (p *Postgres) saveVersion(version int) error {
	tx, err := p.db.Begin()
	if err != nil {
		return err // TODO: warn user
	}

	if _, err := p.db.Exec("TRUNCATE " + tableName + ""); err != nil {
		tx.Rollback()
		return err // TODO: warn user
	}

	if version >= 0 {
		if _, err := p.db.Exec("INSERT INTO "+tableName+" (version) VALUES ($1)", version); err != nil {
			tx.Rollback()
			return err // TODO: warn user
		}
	}

	if err := tx.Commit(); err != nil {
		return err // TODO: warn user
	}

	return nil
}

func (p *Postgres) Version() (int, error) {
	var version uint64
	err := p.db.QueryRow("SELECT version FROM " + tableName + " ORDER BY version DESC LIMIT 1").Scan(&version)
	switch {
	case err == sql.ErrNoRows:
		return database.NilVersion, nil
	case err != nil:
		if e, ok := err.(*pq.Error); ok {
			if e.Code.Name() == "undefined_table" {
				return database.NilVersion, nil
			}
		}
		return 0, err
	default:
		return int(version), nil
	}
}

func (p *Postgres) Drop() error {
	if _, err := p.db.Exec("DROP SCHEMA public cascade "); err != nil {
		return err
	}
	if _, err := p.db.Exec("CREATE SCHEMA public"); err != nil {
		return err
	}
	if err := p.ensureVersionTable(); err != nil {
		return err
	}
	return nil
}

func (p *Postgres) ensureVersionTable() error {
	r := p.db.QueryRow("SELECT count(*) FROM information_schema.tables WHERE table_name = $1 AND table_schema = (SELECT current_schema())", tableName)
	c := 0
	if err := r.Scan(&c); err != nil {
		return err
	}
	if c > 0 {
		return nil
	}
	if _, err := p.db.Exec("CREATE TABLE IF NOT EXISTS " + tableName + " (version bigint not null primary key);"); err != nil {
		return err
	}
	return nil
}

const AdvisoryLockIdSalt uint = 1486364155

// inspired by rails migrations, see https://goo.gl/8o9bCT
func (p *Postgres) generateAdvisoryLockId() (string, error) {
	if p.url == nil {
		return "", ErrNoDatabaseName
	}
	dbname := p.url.Path
	if len(dbname) == 0 {
		return "", ErrNoDatabaseName
	}
	sum := crc32.ChecksumIEEE([]byte(dbname))
	sum = sum * uint32(AdvisoryLockIdSalt)
	return fmt.Sprintf("%v", sum), nil
}
