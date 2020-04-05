package neo4j

import (
	"C" // import C so that we can't compile with CGO_ENABLED=0
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	neturl "net/url"
	"strconv"
	"sync/atomic"

	"github.com/golang-migrate/migrate/v4/database"
	"github.com/hashicorp/go-multierror"
	"github.com/neo4j/neo4j-go-driver/neo4j"
)

func init() {
	db := Neo4j{}
	database.Register("neo4j", &db)
}

const DefaultMigrationsLabel = "SchemaMigration"

var StatementSeparator = []byte(";")

var (
	ErrNilConfig = fmt.Errorf("no config")
)

type Config struct {
	AuthToken       neo4j.AuthToken
	URL             string // if using WithInstance, don't provide auth in the URL, it will be ignored
	MigrationsLabel string
	MultiStatement  bool
}

type Neo4j struct {
	driver neo4j.Driver
	lock   uint32

	// Open and WithInstance need to guarantee that config is never nil
	config *Config
}

func WithInstance(config *Config) (database.Driver, error) {
	if config == nil {
		return nil, ErrNilConfig
	}

	neoDriver, err := neo4j.NewDriver(config.URL, config.AuthToken)
	if err != nil {
		return nil, err
	}

	driver := &Neo4j{
		driver: neoDriver,
		config: config,
	}

	if err := driver.ensureVersionConstraint(); err != nil {
		return nil, err
	}

	return driver, nil
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
	multi := false
	if msQuery != "" {
		multi, err = strconv.ParseBool(uri.Query().Get("x-multi-statement"))
		if err != nil {
			return nil, err
		}
	}
	uri.RawQuery = ""

	return WithInstance(&Config{
		URL:             uri.String(),
		AuthToken:       authToken,
		MigrationsLabel: DefaultMigrationsLabel,
		MultiStatement:  multi,
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
	body, err := ioutil.ReadAll(migration)
	if err != nil {
		return err
	}

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
		statements := bytes.Split(body, StatementSeparator)
		_, err = session.WriteTransaction(func(transaction neo4j.Transaction) (interface{}, error) {
			for _, stmt := range statements {
				trimStmt := bytes.TrimSpace(stmt)
				if len(trimStmt) == 0 {
					continue
				}
				result, err := transaction.Run(string(trimStmt[:]), nil)
				if _, err := neo4j.Collect(result, err); err != nil {
					return nil, err
				}
			}
			return nil, nil
		})
		if err != nil {
			return err
		}
	} else {
		if _, err := neo4j.Collect(session.Run(string(body[:]), nil)); err != nil {
			return err
		}
	}

	return nil
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

	query := fmt.Sprintf("MERGE (sm:%s {version: $version}) SET sm.dirty = $dirty",
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

	query := fmt.Sprintf("MATCH (sm:%s) RETURN sm.version AS version, sm.dirty AS dirty ORDER BY sm.version DESC LIMIT 1",
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

	query := fmt.Sprintf("CREATE CONSTRAINT ON (a:%s) ASSERT a.version IS UNIQUE", n.config.MigrationsLabel)
	if _, err := neo4j.Collect(session.Run(query, nil)); err != nil {
		return err
	}
	return nil
}
