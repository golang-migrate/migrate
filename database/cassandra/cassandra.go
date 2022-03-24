package cassandra

import (
	"errors"
	"fmt"
	"go.uber.org/atomic"
	"io"
	"io/ioutil"
	nurl "net/url"
	"strconv"
	"strings"
	"time"

	"github.com/gocql/gocql"
	"github.com/golang-migrate/migrate/v4/database"
	"github.com/golang-migrate/migrate/v4/database/multistmt"
	"github.com/hashicorp/go-multierror"
)

func init() {
	db := new(Cassandra)
	database.Register("cassandra", db)
}

var (
	multiStmtDelimiter = []byte(";")

	DefaultMultiStatementMaxSize = 10 * 1 << 20 // 10 MB
)

var DefaultMigrationsTable = "schema_migrations"

var (
	ErrNilConfig     = errors.New("no config")
	ErrNoKeyspace    = errors.New("no keyspace provided")
	ErrDatabaseDirty = errors.New("database is dirty")
	ErrClosedSession = errors.New("session is closed")
)

type Config struct {
	MigrationsTable       string
	KeyspaceName          string
	MultiStatementEnabled bool
	MultiStatementMaxSize int
}

type Cassandra struct {
	session  *gocql.Session
	isLocked atomic.Bool

	// Open and WithInstance need to guarantee that config is never nil
	config *Config
}

func WithInstance(session *gocql.Session, config *Config) (database.Driver, error) {
	if config == nil {
		return nil, ErrNilConfig
	} else if len(config.KeyspaceName) == 0 {
		return nil, ErrNoKeyspace
	}

	if session.Closed() {
		return nil, ErrClosedSession
	}

	if len(config.MigrationsTable) == 0 {
		config.MigrationsTable = DefaultMigrationsTable
	}

	if config.MultiStatementMaxSize <= 0 {
		config.MultiStatementMaxSize = DefaultMultiStatementMaxSize
	}

	c := &Cassandra{
		session: session,
		config:  config,
	}

	if err := c.ensureVersionTable(); err != nil {
		return nil, err
	}

	return c, nil
}

func (c *Cassandra) Open(url string) (database.Driver, error) {
	u, err := nurl.Parse(url)
	if err != nil {
		return nil, err
	}

	// Check for missing mandatory attributes
	if len(u.Path) == 0 {
		return nil, ErrNoKeyspace
	}

	cluster := gocql.NewCluster(u.Host)
	cluster.Keyspace = strings.TrimPrefix(u.Path, "/")
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
	if len(u.Query().Get("connect-timeout")) > 0 {
		var connectTimeout time.Duration
		connectTimeout, err = time.ParseDuration(u.Query().Get("connect-timeout"))
		if err != nil {
			return nil, err
		}
		cluster.ConnectTimeout = connectTimeout
	}

	if len(u.Query().Get("sslmode")) > 0 {
		if u.Query().Get("sslmode") != "disable" {
			sslOpts := &gocql.SslOptions{}

			if len(u.Query().Get("sslrootcert")) > 0 {
				sslOpts.CaPath = u.Query().Get("sslrootcert")
			}
			if len(u.Query().Get("sslcert")) > 0 {
				sslOpts.CertPath = u.Query().Get("sslcert")
			}
			if len(u.Query().Get("sslkey")) > 0 {
				sslOpts.KeyPath = u.Query().Get("sslkey")
			}

			if u.Query().Get("sslmode") == "verify-full" {
				sslOpts.EnableHostVerification = true
			}

			cluster.SslOpts = sslOpts
		}
	}

	if len(u.Query().Get("disable-host-lookup")) > 0 {
		if flag, err := strconv.ParseBool(u.Query().Get("disable-host-lookup")); err != nil && flag {
			cluster.DisableInitialHostLookup = true
		} else if err != nil {
			return nil, err
		}
	}

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}

	multiStatementMaxSize := DefaultMultiStatementMaxSize
	if s := u.Query().Get("x-multi-statement-max-size"); len(s) > 0 {
		multiStatementMaxSize, err = strconv.Atoi(s)
		if err != nil {
			return nil, err
		}
	}

	return WithInstance(session, &Config{
		KeyspaceName:          strings.TrimPrefix(u.Path, "/"),
		MigrationsTable:       u.Query().Get("x-migrations-table"),
		MultiStatementEnabled: u.Query().Get("x-multi-statement") == "true",
		MultiStatementMaxSize: multiStatementMaxSize,
	})
}

func (c *Cassandra) Close() error {
	c.session.Close()
	return nil
}

func (c *Cassandra) Lock() error {
	if !c.isLocked.CAS(false, true) {
		return database.ErrLocked
	}
	return nil
}

func (c *Cassandra) Unlock() error {
	if !c.isLocked.CAS(true, false) {
		return database.ErrNotLocked
	}
	return nil
}

func (c *Cassandra) Run(migration io.Reader) error {
	if c.config.MultiStatementEnabled {
		var err error
		if e := multistmt.Parse(migration, multiStmtDelimiter, c.config.MultiStatementMaxSize, func(m []byte) bool {
			tq := strings.TrimSpace(string(m))
			if tq == "" {
				return true
			}
			if e := c.session.Query(tq).Exec(); e != nil {
				err = database.Error{OrigErr: e, Err: "migration failed", Query: m}
				return false
			}
			return true
		}); e != nil {
			return e
		}
		return err
	}

	migr, err := ioutil.ReadAll(migration)
	if err != nil {
		return err
	}
	// run migration
	if err := c.session.Query(string(migr)).Exec(); err != nil {
		// TODO: cast to Cassandra error and get line number
		return database.Error{OrigErr: err, Err: "migration failed", Query: migr}
	}
	return nil
}

func (c *Cassandra) SetVersion(version int, dirty bool) error {
	// DELETE instead of TRUNCATE because AWS Keyspaces does not support it
	// see: https://docs.aws.amazon.com/keyspaces/latest/devguide/cassandra-apis.html
	squery := `SELECT version FROM "` + c.config.MigrationsTable + `"`
	dquery := `DELETE FROM "` + c.config.MigrationsTable + `" WHERE version = ?`
	iter := c.session.Query(squery).Iter()
	var previous int
	for iter.Scan(&previous) {
		if err := c.session.Query(dquery, previous).Exec(); err != nil {
			return &database.Error{OrigErr: err, Query: []byte(dquery)}
		}
	}
	if err := iter.Close(); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(squery)}
	}

	// Also re-write the schema version for nil dirty versions to prevent
	// empty schema version for failed down migration on the first migration
	// See: https://github.com/golang-migrate/migrate/issues/330
	if version >= 0 || (version == database.NilVersion && dirty) {
		query := `INSERT INTO "` + c.config.MigrationsTable + `" (version, dirty) VALUES (?, ?)`
		if err := c.session.Query(query, version, dirty).Exec(); err != nil {
			return &database.Error{OrigErr: err, Query: []byte(query)}
		}
	}

	return nil
}

// Return current keyspace version
func (c *Cassandra) Version() (version int, dirty bool, err error) {
	query := `SELECT version, dirty FROM "` + c.config.MigrationsTable + `" LIMIT 1`
	err = c.session.Query(query).Scan(&version, &dirty)
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

func (c *Cassandra) Drop() error {
	// select all tables in current schema
	query := fmt.Sprintf(`SELECT table_name from system_schema.tables WHERE keyspace_name='%s'`, c.config.KeyspaceName)
	iter := c.session.Query(query).Iter()
	var tableName string
	for iter.Scan(&tableName) {
		err := c.session.Query(fmt.Sprintf(`DROP TABLE %s`, tableName)).Exec()
		if err != nil {
			return err
		}
	}

	return nil
}

// ensureVersionTable checks if versions table exists and, if not, creates it.
// Note that this function locks the database, which deviates from the usual
// convention of "caller locks" in the Cassandra type.
func (c *Cassandra) ensureVersionTable() (err error) {
	if err = c.Lock(); err != nil {
		return err
	}

	defer func() {
		if e := c.Unlock(); e != nil {
			if err == nil {
				err = e
			} else {
				err = multierror.Append(err, e)
			}
		}
	}()

	err = c.session.Query(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (version bigint, dirty boolean, PRIMARY KEY(version))", c.config.MigrationsTable)).Exec()
	if err != nil {
		return err
	}
	if _, _, err = c.Version(); err != nil {
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
