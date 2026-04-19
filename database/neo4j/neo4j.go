package neo4j

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	neturl "net/url"
	"strconv"
	"sync/atomic"
	"time"

	"golang.org/x/mod/semver"

	"github.com/golang-migrate/migrate/v4/database"
	"github.com/golang-migrate/migrate/v4/database/multistmt"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
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
	driver neo4j.DriverWithContext
	lock   uint32

	// Open and WithInstance need to guarantee that config is never nil
	config *Config
}

func WithInstance(driver neo4j.DriverWithContext, config *Config) (database.Driver, error) {
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
	msQuery := uri.Query().Get("x-multi-statement")

	multi := false
	if msQuery != "" {
		multi, err = strconv.ParseBool(uri.Query().Get("x-multi-statement"))
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

	driver, err := neo4j.NewDriverWithContext(uri.String(), authToken)
	if err != nil {
		return nil, err
	}

	if err = func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return driver.VerifyConnectivity(ctx)
	}(); err != nil {
		_ = driver.Close(context.Background())
		return nil, err
	}

	return WithInstance(driver, &Config{
		MigrationsLabel:       DefaultMigrationsLabel,
		MultiStatement:        multi,
		MultiStatementMaxSize: multiStatementMaxSize,
	})
}

func (n *Neo4j) Close() error {
	return n.driver.Close(context.Background())
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
	ctx := context.Background()
	session := n.driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer func() {
		if cerr := session.Close(ctx); cerr != nil {
			err = errors.Join(err, cerr)
		}
	}()

	if n.config.MultiStatement {
		tx, err := session.BeginTransaction(ctx)
		if err != nil {
			return err
		}
		defer func() {
			if cerr := tx.Close(ctx); cerr != nil {
				err = errors.Join(err, cerr)
			}
		}()

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

			result, err := tx.Run(ctx, string(trimStmt), nil)
			if _, err := neo4j.CollectWithContext(ctx, result, err); err != nil {
				stmtRunErr = err
				return false
			}
			return true
		}); err != nil {
			return err
		}
		if stmtRunErr != nil {
			return stmtRunErr
		}
		return tx.Commit(ctx)
	}

	body, err := io.ReadAll(migration)
	if err != nil {
		return err
	}

	res, err := session.Run(ctx, string(body[:]), nil)
	_, err = neo4j.CollectWithContext(ctx, res, err)
	return err
}

func (n *Neo4j) SetVersion(version int, dirty bool) (err error) {
	ctx := context.Background()
	session := n.driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer func() {
		if cerr := session.Close(ctx); cerr != nil {
			err = errors.Join(err, cerr)
		}
	}()

	query := fmt.Sprintf("MERGE (sm:%s {version: $version}) SET sm.dirty = $dirty, sm.ts = datetime()",
		n.config.MigrationsLabel)
	res, err := session.Run(ctx, query, map[string]interface{}{"version": version, "dirty": dirty})
	_, err = neo4j.CollectWithContext(ctx, res, err)
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
	ctx := context.Background()
	session := n.driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
	defer func() {
		if cerr := session.Close(ctx); cerr != nil {
			err = errors.Join(err, cerr)
		}
	}()

	query := fmt.Sprintf(`MATCH (sm:%s) RETURN sm.version AS version, sm.dirty AS dirty
ORDER BY COALESCE(sm.ts, datetime({year: 0})) DESC, sm.version DESC LIMIT 1`,
		n.config.MigrationsLabel)

	tx, err := session.BeginTransaction(ctx)
	if err != nil {
		return database.NilVersion, false, err
	}
	defer func() {
		if cerr := tx.Close(ctx); cerr != nil {
			err = errors.Join(err, cerr)
		}
	}()

	result, err := tx.Run(ctx, query, nil)
	if err != nil {
		return database.NilVersion, false, err
	}
	if result.Next(ctx) {
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

		return mr.Version, mr.Dirty, nil
	}

	if err := result.Err(); err != nil {
		return database.NilVersion, false, err
	}

	return database.NilVersion, false, err
}

func (n *Neo4j) Drop() (err error) {
	ctx := context.Background()
	session := n.driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer func() {
		if cerr := session.Close(ctx); cerr != nil {
			err = errors.Join(err, cerr)
		}
	}()

	res, err := session.Run(ctx, "MATCH (n) DETACH DELETE n", nil)
	if _, err := neo4j.CollectWithContext(ctx, res, err); err != nil {
		return err
	}
	return nil
}

func (n *Neo4j) ensureVersionConstraint() (err error) {
	ctx := context.Background()
	session := n.driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer func() {
		if cerr := session.Close(ctx); cerr != nil {
			err = errors.Join(err, cerr)
		}
	}()

	var neo4jVersion string
	result, err := session.Run(ctx, "CALL dbms.components() YIELD name, versions UNWIND versions AS version WHERE name = 'Neo4j Kernel' RETURN version LIMIT 1", nil)
	res, err := neo4j.CollectWithContext(ctx, result, err)
	if err != nil {
		return err
	}
	if len(res) > 0 && len(res[0].Values) > 0 {
		if v, ok := res[0].Values[0].(string); ok {
			neo4jVersion = semver.Major("v" + v)
		}
	}

	// Use IF NOT EXISTS to avoid duplicate constraint errors and to correctly
	// detect whether the constraint exists independent of the label's existence.
	var query string
	switch neo4jVersion {
	case "v5":
		query = fmt.Sprintf("CREATE CONSTRAINT IF NOT EXISTS FOR (a:%s) REQUIRE a.version IS UNIQUE", n.config.MigrationsLabel)
	case "v4":
		query = fmt.Sprintf("CREATE CONSTRAINT IF NOT EXISTS ON (a:%s) ASSERT a.version IS UNIQUE", n.config.MigrationsLabel)
	default:
		return fmt.Errorf("unsupported neo4j version %v", neo4jVersion)
	}

	result, err = session.Run(ctx, query, nil)
	if _, err := neo4j.CollectWithContext(ctx, result, err); err != nil {
		return err
	}
	return nil
}
