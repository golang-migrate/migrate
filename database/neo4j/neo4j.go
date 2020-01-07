package neo4j

import (
	"fmt"
	"io"
	"io/ioutil"
	neturl "net/url"

	"github.com/golang-migrate/migrate/v4/database"
	"github.com/neo4j/neo4j-go-driver/neo4j"
)

func init() {
	db := Neo4j{}
	database.Register("bolt", &db)
	database.Register("neo4j", &db)
}

var DefaultMigrationsLabel = "SchemaMigration"

var (
	ErrNilConfig      = fmt.Errorf("no config")
)

type Config struct {
	AuthToken neo4j.AuthToken
	URL string
	MigrationsLabel string
}

type Neo4j struct {
	driver neo4j.Driver

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

	return WithInstance(&Config{
		URL:             url,
		AuthToken:       authToken,
		MigrationsLabel: DefaultMigrationsLabel,
	})
}

func (n *Neo4j) Close() error {
	return n.driver.Close()
}

func (n *Neo4j) Lock() error {
	return nil
}

func (n *Neo4j) Unlock() error {
	return nil
}

func (n *Neo4j) Run(migration io.Reader) error {
	body, err := ioutil.ReadAll(migration)
	if err != nil {
		return err
	}

	session, err := n.driver.Session(neo4j.AccessModeWrite)
	if err != nil {
		return err
	}
	defer session.Close()

	_, err = session.Run(string(body[:]), nil)
	return err
}

func (n *Neo4j) SetVersion(version int, dirty bool) error {
	session, err := n.driver.Session(neo4j.AccessModeRead)
	if err != nil {
		return err
	}
	defer session.Close()

	_, err = session.Run("MERGE (sm:$migration {version: $version, dirty: $dirty})",
			map[string]interface{}{"migration": n.config.MigrationsLabel, "version": version, "dirty": dirty})
	return err
}

func (n *Neo4j) Version() (version int, dirty bool, err error) {
	session, err := n.driver.Session(neo4j.AccessModeRead)
	if err != nil {
		return -1, false, err
	}
	defer session.Close()

	result, err := session.Run("MATCH (sm:$migration) RETURN sm.version, sm.dirty ORDER BY sm.version DESC LIMIT 1",
			map[string]interface{}{"migration": n.config.MigrationsLabel})
	if err != nil {
		return -1, false, err
	}
	if result.Next() {
		versionResult, ok := result.Record().Get("version")
		if !ok {
			version = -1
		} else {
			version = versionResult.(int)
		}
	} else {
		version = -1
	}

	return version, dirty, nil
}

func (n *Neo4j) Drop() error {
	session, err := n.driver.Session(neo4j.AccessModeWrite); if err != nil {
		return err
	}
	defer session.Close()

	_, err = session.Run("MATCH (n) DETACH DELETE n", map[string]interface{}{})
	return err
}

func (n *Neo4j) ensureVersionConstraint() (err error) {
	session, err := n.driver.Session(neo4j.AccessModeWrite); if err != nil {
		return err
	}
	defer session.Close()

	_, err = session.Run("CREATE CONSTRAINT ON (a:$migration) ASSERT a.version IS UNIQUE",
		map[string]interface{}{"migration": n.config.MigrationsLabel})
	return err
}

