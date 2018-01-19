package cassandra

import (
	"fmt"
	"io"
	"io/ioutil"
	nurl "net/url"
	"strconv"
	"time"

	"github.com/gocql/gocql"
	"github.com/golang-migrate/migrate/database"
)

func init() {
	db := new(Cassandra)
	database.Register("cassandra", db)
}

var DefaultMigrationsTable = "schema_migrations"
var dbLocked = false

var (
	ErrNilConfig     = fmt.Errorf("no config")
	ErrNoKeyspace    = fmt.Errorf("no keyspace provided")
	ErrDatabaseDirty = fmt.Errorf("database is dirty")
)

type Config struct {
	MigrationsTable string
	KeyspaceName    string
}

type Cassandra struct {
	session  *gocql.Session
	isLocked bool

	// Open and WithInstance need to guarantee that config is never nil
	config *Config
}

func (p *Cassandra) Open(url string) (database.Driver, error) {
	u, err := nurl.Parse(url)
	if err != nil {
		return nil, err
	}

	// Check for missing mandatory attributes
	if len(u.Path) == 0 {
		return nil, ErrNoKeyspace
	}

	migrationsTable := u.Query().Get("x-migrations-table")
	if len(migrationsTable) == 0 {
		migrationsTable = DefaultMigrationsTable
	}

	p.config = &Config{
		KeyspaceName:    u.Path,
		MigrationsTable: migrationsTable,
	}

	cluster := gocql.NewCluster(u.Host)
	cluster.Keyspace = u.Path[1:len(u.Path)]
	cluster.Consistency = gocql.All
	cluster.Timeout = 1 * time.Minute

	if len(u.Query().Get("username")) > 0 && len(u.Query().Get("password")) > 0 {
		authenticator := gocql.PasswordAuthenticator{
			Username: u.Query().Get("username"),
			Password: u.Query().Get("password"),
		}
		cluster.Authenticator = authenticator
	}

	// Retrieve query string configuration
	if len(u.Query().Get("consistency")) > 0 {
		var consistency gocql.Consistency
		consistency, err = parseConsistency(u.Query().Get("consistency"))
		if err != nil {
			return nil, err
		}

		cluster.Consistency = consistency
	}
	if len(u.Query().Get("protocol")) > 0 {
		var protoversion int
		protoversion, err = strconv.Atoi(u.Query().Get("protocol"))
		if err != nil {
			return nil, err
		}
		cluster.ProtoVersion = protoversion
	}
	if len(u.Query().Get("timeout")) > 0 {
		var timeout time.Duration
		timeout, err = time.ParseDuration(u.Query().Get("timeout"))
		if err != nil {
			return nil, err
		}
		cluster.Timeout = timeout
	}

	p.session, err = cluster.CreateSession()

	if err != nil {
		return nil, err
	}

	if err := p.ensureVersionTable(); err != nil {
		return nil, err
	}

	return p, nil
}

func (p *Cassandra) Close() error {
	p.session.Close()
	return nil
}

func (p *Cassandra) Lock() error {
	if dbLocked {
		return database.ErrLocked
	}
	dbLocked = true
	return nil
}

func (p *Cassandra) Unlock() error {
	dbLocked = false
	return nil
}

func (p *Cassandra) Run(migration io.Reader) error {
	migr, err := ioutil.ReadAll(migration)
	if err != nil {
		return err
	}
	// run migration
	query := string(migr[:])
	if err := p.session.Query(query).Exec(); err != nil {
		// TODO: cast to Cassandra error and get line number
		return database.Error{OrigErr: err, Err: "migration failed", Query: migr}
	}

	return nil
}

func (p *Cassandra) SetVersion(version int, dirty bool) error {
	query := `TRUNCATE "` + p.config.MigrationsTable + `"`
	if err := p.session.Query(query).Exec(); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}
	if version >= 0 {
		query = `INSERT INTO "` + p.config.MigrationsTable + `" (version, dirty) VALUES (?, ?)`
		if err := p.session.Query(query, version, dirty).Exec(); err != nil {
			return &database.Error{OrigErr: err, Query: []byte(query)}
		}
	}

	return nil
}

// Return current keyspace version
func (p *Cassandra) Version() (version int, dirty bool, err error) {
	query := `SELECT version, dirty FROM "` + p.config.MigrationsTable + `" LIMIT 1`
	err = p.session.Query(query).Scan(&version, &dirty)
	switch {
	case err == gocql.ErrNotFound:
		return database.NilVersion, false, nil

	case err != nil:
		if _, ok := err.(*gocql.Error); ok {
			return database.NilVersion, false, nil
		}
		return 0, false, &database.Error{OrigErr: err, Query: []byte(query)}

	default:
		return version, dirty, nil
	}
}

func (p *Cassandra) Drop() error {
	// select all tables in current schema
	query := fmt.Sprintf(`SELECT table_name from system_schema.tables WHERE keyspace_name='%s'`, p.config.KeyspaceName[1:]) // Skip '/' character
	iter := p.session.Query(query).Iter()
	var tableName string
	for iter.Scan(&tableName) {
		err := p.session.Query(fmt.Sprintf(`DROP TABLE %s`, tableName)).Exec()
		if err != nil {
			return err
		}
	}
	// Re-create the version table
	if err := p.ensureVersionTable(); err != nil {
		return err
	}
	return nil
}

// Ensure version table exists
func (p *Cassandra) ensureVersionTable() error {
	err := p.session.Query(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (version bigint, dirty boolean, PRIMARY KEY(version))", p.config.MigrationsTable)).Exec()
	if err != nil {
		return err
	}
	if _, _, err = p.Version(); err != nil {
		return err
	}
	return nil
}

// ParseConsistency wraps gocql.ParseConsistency
// to return an error instead of a panicking.
func parseConsistency(consistencyStr string) (consistency gocql.Consistency, err error) {
	defer func() {
		if r := recover(); r != nil {
			var ok bool
			err, ok = r.(error)
			if !ok {
				err = fmt.Errorf("Failed to parse consistency \"%s\": %v", consistencyStr, r)
			}
		}
	}()
	consistency = gocql.ParseConsistency(consistencyStr)

	return consistency, nil
}
