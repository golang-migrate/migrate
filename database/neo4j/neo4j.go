package neo4j

import (
	"C" // import C so that we can't compile with CGO_ENABLED=0
	"fmt"
	"github.com/hashicorp/go-multierror"
	"io"
	"io/ioutil"
	neturl "net/url"
	"sync/atomic"

	"github.com/golang-migrate/migrate/v4/database"
	"github.com/neo4j/neo4j-go-driver/neo4j"
)

func init() {
	db := Neo4j{}
	database.Register("neo4j", &db)
}

var DefaultMigrationsLabel = "SchemaMigration"

var (
	ErrNilConfig = fmt.Errorf("no config")
)

type Config struct {
	AuthToken       neo4j.AuthToken
	URL             string // if using WithInstance, don't provide auth in the URL, it will be ignored
	MigrationsLabel string
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

	return WithInstance(&Config{
		URL:             uri.String(),
		AuthToken:       authToken,
		MigrationsLabel: DefaultMigrationsLabel,
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

	result, err := session.Run(string(body[:]), nil)
	if err != nil {
		return err
	}
	if err = result.Err(); err != nil {
		return err
	}
	return nil
}

func (n *Neo4j) SetVersion(version int, dirty bool) (err error) {
	session, err := n.driver.Session(neo4j.AccessModeRead)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := session.Close(); cerr != nil {
			err = multierror.Append(err, cerr)
		}
	}()

	query := fmt.Sprintf("MERGE (sm:%s {version: $version, dirty: $dirty})",
		n.config.MigrationsLabel)
	result, err := session.Run(query, map[string]interface{}{"version": version, "dirty": dirty})
	if err != nil {
		return err
	}
	if err = result.Err(); err != nil {
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
		return -1, false, err
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
				mr.Version = -1
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
		return -1, false, err
	}
	if result == nil {
		return -1, false, err
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

	_, err = session.Run("MATCH (n) DETACH DELETE n", nil)
	return err
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
	result, err := session.Run(query, nil)
	if err != nil {
		return err
	}
	if err = result.Err(); err != nil {
		return err
	}
	return nil
}
