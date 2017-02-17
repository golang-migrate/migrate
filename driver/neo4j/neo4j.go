// Package neo4j implements the Driver interface.
package neo4j

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/jmcvetta/neoism"
	"github.com/mattes/migrate/driver"
	"github.com/mattes/migrate/file"
	"github.com/mattes/migrate/migrate/direction"
	"strings"
)

type Driver struct {
	db *neoism.Database
}

const labelName = "SchemaMigration"

func (driver *Driver) Initialize(url string) error {
	url = strings.Replace(url, "neo4j", "http", 1)

	db, err := neoism.Connect(url)
	if err != nil {
		return err
	}

	driver.db = db

	if err := driver.ensureVersionConstraintExists(); err != nil {
		return err
	}
	return nil
}

func (driver *Driver) Close() error {
	driver.db = nil
	return nil
}

func (driver *Driver) FilenameExtension() string {
	return "cql"
}

func (driver *Driver) ensureVersionConstraintExists() error {
	uc, _ := driver.db.UniqueConstraints("SchemaMigration", "version")
	if len(uc) == 0 {
		_, err := driver.db.CreateUniqueConstraint("SchemaMigration", "version")
		return err
	}
	return nil
}

func (driver *Driver) setVersion(d direction.Direction, v uint64, invert bool) error {

	cqUp := neoism.CypherQuery{
		Statement:  `CREATE (n:SchemaMigration {version: {Version}}) RETURN n`,
		Parameters: neoism.Props{"Version": v},
	}

	cqDown := neoism.CypherQuery{
		Statement:  `MATCH (n:SchemaMigration {version: {Version}}) DELETE n`,
		Parameters: neoism.Props{"Version": v},
	}

	var cq neoism.CypherQuery
	switch d {
	case direction.Up:
		if invert {
			cq = cqDown
		} else {
			cq = cqUp
		}
	case direction.Down:
		if invert {
			cq = cqUp
		} else {
			cq = cqDown
		}
	}
	return driver.db.Cypher(&cq)
}

func (driver *Driver) Migrate(f file.File, pipe chan interface{}) {
	var err error

	defer func() {
		if err != nil {
			// Invert version direction if we couldn't apply the changes for some reason.
			if err := driver.setVersion(f.Direction, f.Version, true); err != nil {
				pipe <- err
			}
			pipe <- err
		}
		close(pipe)
	}()

	pipe <- f

	if err = driver.setVersion(f.Direction, f.Version, false); err != nil {
		pipe <- err
		return
	}

	if err = f.ReadContent(); err != nil {
		pipe <- err
		return
	}

	cQueries := []*neoism.CypherQuery{}

	// Neoism doesn't support multiple statements per query.
	cqlStmts := bytes.Split(f.Content, []byte(";"))

	for _, cqlStmt := range cqlStmts {
		cqlStmt = bytes.TrimSpace(cqlStmt)
		if len(cqlStmt) > 0 {
			cq := neoism.CypherQuery{Statement: string(cqlStmt)}
			cQueries = append(cQueries, &cq)
		}
	}

	var tx *neoism.Tx

	tx, err = driver.db.Begin(cQueries)
	if err != nil {
		pipe <- err
		for _, err := range tx.Errors {
			pipe <- errors.New(fmt.Sprintf("%v", err.Message))
		}
		if err = tx.Rollback(); err != nil {
			pipe <- err
		}
		return
	}

	if err = tx.Commit(); err != nil {
		pipe <- err
		for _, err := range tx.Errors {
			pipe <- errors.New(fmt.Sprintf("%v", err.Message))
		}
		return
	}
}

func (driver *Driver) Version() (uint64, error) {
	res := []struct {
		Version uint64 `json:"n.version"`
	}{}

	cq := neoism.CypherQuery{
		Statement: `MATCH (n:SchemaMigration)
      RETURN n.version ORDER BY n.version DESC LIMIT 1`,
		Result: &res,
	}

	if err := driver.db.Cypher(&cq); err != nil || len(res) == 0 {
		return 0, err
	}
	return res[0].Version, nil
}

func init() {
	driver.RegisterDriver("neo4j", &Driver{})
}
