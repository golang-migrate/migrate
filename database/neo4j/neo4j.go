package neo4j

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	neturl "net/url"
	"strconv"
	"sync/atomic"

	"github.com/golang-migrate/migrate/v4/database"
	"github.com/golang-migrate/migrate/v4/database/multistmt"
	"github.com/hashicorp/go-multierror"
	"github.com/neo4j/neo4j-go-driver/neo4j"
)

func init() {
	db := Neo4j{}
	database.Register("neo4j", &db)
}

const DefaultMigrationsLabel = "SchemaMigration"

var (
	StatementSeparator           = []byte(";")
	DefaultMultiStatementMaxSize = 10 * 1 << 20 // 10 MB
)

var (
	ErrNilConfig = fmt.Errorf("no config")
)

type Config struct {
	MigrationsLabel       string
	MultiStatement        bool
	MultiStatementMaxSize int
}

type Neo4j struct {
	driver neo4j.Driver
	lock   uint32

	// Open and WithInstance need to guarantee that config is never nil
	config *Config
}

func WithInstance(driver neo4j.Driver, config *Config) (database.Driver, error) {
	if config == nil {
		return nil, ErrNilConfig
	}

	nDriver := &Neo4j{
		driver: driver,
		config: config,
	}

	if err := nDriver.ensureVersionConstraint(); err != nil {
		return nil, err
	}

	return nDriver, nil
}

func (n *Neo4j) Open(url string) (database.Driver, error) {
	uri, err := neturl.Parse(url)
	if err != nil {
		return nil, err
	}
	password, _ := uri.User.Password()
	authToken := neo4j.BasicAuth(uri.User.Username(), password, "")
	uri.User = nil
	uri.Scheme = "bolt"
	msQuery := uri.Query().Get("x-multi-statement")

	// Whether to turn on/off TLS encryption.
	tlsEncrypted := uri.Query().Get("x-tls-encrypted")
	multi := false
	encrypted := false
	if msQuery != "" {
		multi, err = strconv.ParseBool(uri.Query().Get("x-multi-statement"))
		if err != nil {
			return nil, err
		}
	}

	if tlsEncrypted != "" {
		encrypted, err = strconv.ParseBool(tlsEncrypted)
		if err != nil {
			return nil, err
		}
	}

	multiStatementMaxSize := DefaultMultiStatementMaxSize
	if s := uri.Query().Get("x-multi-statement-max-size"); s != "" {
		multiStatementMaxSize, err = strconv.Atoi(s)
		if err != nil {
			return nil, err
		}
	}

	uri.RawQuery = ""

	driver, err := neo4j.NewDriver(uri.String(), authToken, func(config *neo4j.Config) {
		config.Encrypted = encrypted
	})
	if err != nil {
		return nil, err
	}

	return WithInstance(driver, &Config{
		MigrationsLabel:       DefaultMigrationsLabel,
		MultiStatement:        multi,
		MultiStatementMaxSize: multiStatementMaxSize,
	})
}

func (n *Neo4j) Close() error {
	return n.driver.Close()
}

// local locking in order to pass tests, Neo doesn't support database locking
func (n *Neo4j) Lock() error {
	if !atomic.CompareAndSwapUint32(&n.lock, 0, 1) {
		return database.ErrLocked
	}

	return nil
}

func (n *Neo4j) Unlock() error {
	if !atomic.CompareAndSwapUint32(&n.lock, 1, 0) {
		return database.ErrNotLocked
	}
	return nil
}

func (n *Neo4j) Run(migration io.Reader) (err error) {
	session, err := n.driver.Session(neo4j.AccessModeWrite)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := session.Close(); cerr != nil {
			err = multierror.Append(err, cerr)
		}
	}()

	if n.config.MultiStatement {
		_, err = session.WriteTransaction(func(transaction neo4j.Transaction) (interface{}, error) {
			var stmtRunErr error
			if err := multistmt.Parse(migration, StatementSeparator, n.config.MultiStatementMaxSize, func(stmt []byte) bool {
				trimStmt := bytes.TrimSpace(stmt)
				if len(trimStmt) == 0 {
					return true
				}
				trimStmt = bytes.TrimSuffix(trimStmt, StatementSeparator)
				if len(trimStmt) == 0 {
					return true
				}

				result, err := transaction.Run(string(trimStmt), nil)
				if _, err := neo4j.Collect(result, err); err != nil {
					stmtRunErr = err
					return false
				}
				return true
			}); err != nil {
				return nil, err
			}
			return nil, stmtRunErr
		})
		return err
	}

	body, err := ioutil.ReadAll(migration)
	if err != nil {
		return err
	}

	_, err = neo4j.Collect(session.Run(string(body[:]), nil))
	return err
}

func (n *Neo4j) SetVersion(version int, dirty bool) (err error) {
	session, err := n.driver.Session(neo4j.AccessModeWrite)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := session.Close(); cerr != nil {
			err = multierror.Append(err, cerr)
		}
	}()

	query := fmt.Sprintf("MERGE (sm:%s {version: $version}) SET sm.dirty = $dirty, sm.ts = datetime()",
		n.config.MigrationsLabel)
	_, err = neo4j.Collect(session.Run(query, map[string]interface{}{"version": version, "dirty": dirty}))
	if err != nil {
		return err
	}
	return nil
}

type MigrationRecord struct {
	Version int
	Dirty   bool
}

func (n *Neo4j) Version() (version int, dirty bool, err error) {
	session, err := n.driver.Session(neo4j.AccessModeRead)
	if err != nil {
		return database.NilVersion, false, err
	}
	defer func() {
		if cerr := session.Close(); cerr != nil {
			err = multierror.Append(err, cerr)
		}
	}()

	query := fmt.Sprintf(`MATCH (sm:%s) RETURN sm.version AS version, sm.dirty AS dirty
ORDER BY COALESCE(sm.ts, datetime({year: 0})) DESC, sm.version DESC LIMIT 1`,
		n.config.MigrationsLabel)
	result, err := session.ReadTransaction(func(transaction neo4j.Transaction) (interface{}, error) {
		result, err := transaction.Run(query, nil)
		if err != nil {
			return nil, err
		}
		if result.Next() {
			record := result.Record()
			mr := MigrationRecord{}
			versionResult, ok := record.Get("version")
			if !ok {
				mr.Version = database.NilVersion
			} else {
				mr.Version = int(versionResult.(int64))
			}

			dirtyResult, ok := record.Get("dirty")
			if ok {
				mr.Dirty = dirtyResult.(bool)
			}

			return mr, nil
		}
		return nil, result.Err()
	})
	if err != nil {
		return database.NilVersion, false, err
	}
	if result == nil {
		return database.NilVersion, false, err
	}
	mr := result.(MigrationRecord)
	return mr.Version, mr.Dirty, err
}

func (n *Neo4j) Drop() (err error) {
	session, err := n.driver.Session(neo4j.AccessModeWrite)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := session.Close(); cerr != nil {
			err = multierror.Append(err, cerr)
		}
	}()

	if _, err := neo4j.Collect(session.Run("MATCH (n) DETACH DELETE n", nil)); err != nil {
		return err
	}
	return nil
}

func (n *Neo4j) ensureVersionConstraint() (err error) {
	session, err := n.driver.Session(neo4j.AccessModeWrite)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := session.Close(); cerr != nil {
			err = multierror.Append(err, cerr)
		}
	}()

	/**
	Get constraint and check to avoid error duplicate
	using db.labels() to support Neo4j 3 and 4.
	Neo4J 3 doesn't support db.constraints() YIELD name
	*/
	res, err := neo4j.Collect(session.Run(fmt.Sprintf("CALL db.labels() YIELD label WHERE label=\"%s\" RETURN label", n.config.MigrationsLabel), nil))
	if err != nil {
		return err
	}
	if len(res) == 1 {
		return nil
	}

	query := fmt.Sprintf("CREATE CONSTRAINT ON (a:%s) ASSERT a.version IS UNIQUE", n.config.MigrationsLabel)
	if _, err := neo4j.Collect(session.Run(query, nil)); err != nil {
		return err
	}
	return nil
}
