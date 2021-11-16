package dgraph

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	nurl "net/url"
	"strings"

	"github.com/dgraph-io/dgo/v210"
	"github.com/dgraph-io/dgo/v210/protos/api"
	"github.com/golang-migrate/migrate/v4/database"
	"github.com/hashicorp/go-multierror"
	jsoniter "github.com/json-iterator/go"
	"github.com/machinebox/graphql"
	uatomic "go.uber.org/atomic"
	"google.golang.org/grpc"
)

const (
	unlockedVal = 0
	lockedVal   = 1
)

// Driver errors
var (
	ErrNilConfig     = errors.New("no config")
	ErrNoSchema      = errors.New("no schema")
	ErrDatabaseDirty = errors.New("database is dirty")
	ErrLockHeld      = errors.New("unable to obtain lock")
	ErrLockNotHeld   = errors.New("unable to release already released lock")
)

var DqlJson = jsoniter.Config{TagKey: "dql"}.Froze()
var GraphQLJson = jsoniter.Config{TagKey: "graphql"}.Froze()

func init() {
	db := DGraph{}
	database.Register("dgraph", &db)
}

const DQLSchema = `
<version>: int .
<dirty>: bool .
type <Migration> {
	version
	dirty
}`

const GraphQLSchema = `
<Migration.version>: int .
<Migration.dirty>: bool .
type <Migration> {
	Migration.version
	Migration.dirty
}`

const GetVersionDQL = `{
	getVersion(func: type(Migration), first: 1) {
		version
		dirty
	}
}`

const GetVersionGraphQL = `{
	getVersion(func: type(Migration), first: 1) {
		Migration.version
		Migration.dirty
	}
}`

type MutationOperation struct {
	SetJson    map[string]interface{} `json:"set_json"`
	DeleteJson map[string]interface{} `json:"delete_json"`
}

func (m *MutationOperation) ToMutation() (*api.Mutation, error) {
	mutation := &api.Mutation{}

	if len(m.SetJson) > 0 {
		setJson, err := json.Marshal(m.SetJson)
		if err != nil {
			return nil, err
		}
		mutation.SetJson = setJson
	}
	if len(m.DeleteJson) > 0 {
		deleteJson, err := json.Marshal(m.DeleteJson)
		if err != nil {
			return nil, err
		}
		mutation.DeleteJson = deleteJson
	}
	return mutation, nil
}

type UpsertOperation struct {
	Query     string               `json:"query"`
	Mutations []*MutationOperation `json:"mutations"`
}

type Operation_DropOp string

const (
	Operation_NONE Operation_DropOp = "NONE"
	Operation_ALL  Operation_DropOp = "ALL"
	Operation_DATA Operation_DropOp = "DATA"
	Operation_ATTR Operation_DropOp = "ATTR"
	Operation_TYPE Operation_DropOp = "TYPE"
)

type AlterOperation struct {
	Schema    []string         `json:"schema"`
	DropAttr  string           `json:"drop_attr"`
	DropValue string           `json:"drop_value"`
	DropOp    Operation_DropOp `json:"drop_op"`
	DropAll   bool             `json:"drop_all"`
}

func (a *AlterOperation) ToOperation() *api.Operation {
	operation := &api.Operation{
		Schema:    strings.Join(a.Schema, "\n"),
		DropAttr:  a.DropAttr,
		DropAll:   a.DropAll,
		DropValue: a.DropValue,
	}

	switch a.DropOp {
	case Operation_NONE:
		operation.DropOp = api.Operation_NONE
	case Operation_ALL:
		operation.DropOp = api.Operation_ALL
	case Operation_DATA:
		operation.DropOp = api.Operation_DATA
	case Operation_ATTR:
		operation.DropOp = api.Operation_ATTR
	case Operation_TYPE:
		operation.DropOp = api.Operation_TYPE
	}
	return operation
}

type Operations struct {
	Alter    []*AlterOperation    `json:"alter"`
	Mutation []*MutationOperation `json:"mutation"`
	Upsert   []*UpsertOperation   `json:"upsert"`
}

type Migration struct {
	Uid     string `graphql:"uid" dql:"uid"`
	Version int    `graphql:"Migration.version" dql:"version"`
	Dirty   bool   `graphql:"Migration.dirty" dql:"dirty"`
	Type    string `graphql:"dgraph.type" dql:"dgraph.type"`
}

// Config used for a DGraph instance
type Config struct {
	GraphQL bool // Should the version be available in GraphQL? Uses GraphQL naming conventions
}

// DGraph implements database.Driver for DGraph
type DGraph struct {
	db *DB

	config *Config

	lock *uatomic.Uint32
}

type DB struct {
	graphql *graphql.Client
	dql     *dgo.Dgraph
}

func NewDB(graphql *graphql.Client, dql *dgo.Dgraph) *DB {
	return &DB{
		graphql: graphql,
		dql:     dql,
	}
}

// WithInstance implements database.Driver
func WithInstance(instance *DB, config *Config) (database.Driver, error) {
	if config == nil {
		return nil, ErrNilConfig
	}

	dx := &DGraph{
		db:     instance,
		config: config,
		lock:   uatomic.NewUint32(unlockedVal),
	}

	if err := dx.ensureMigrationNode(config.GraphQL); err != nil {
		return nil, err
	}

	return dx, nil
}

func (d *DGraph) ensureMigrationNode(conformGraphQL bool) (err error) {
	if err = d.Lock(); err != nil {
		return err
	}

	defer func() {
		if e := d.Unlock(); e != nil {
			if err == nil {
				err = e
			} else {
				err = multierror.Append(err, e)
			}
		}
	}()

	ctx := context.Background()

	if conformGraphQL {
		err = d.db.dql.Alter(ctx, &api.Operation{Schema: GraphQLSchema})
	} else {
		err = d.db.dql.Alter(ctx, &api.Operation{Schema: DQLSchema})
	}

	if err != nil {
		return err
	}

	return nil
}

func (d *DGraph) Open(url string) (database.Driver, error) {
	purl, err := nurl.Parse(url)
	if err != nil {
		return nil, err
	}

	hostname := purl.Hostname()
	dqlPort := purl.Port()
	conformGraphql := purl.Query().Has("graphql")
	graphqlPort := purl.Query().Get("graphqlPort")
	sslmode := purl.Query().Get("sslmode")
	if sslmode == "disable" {
		sslmode = "http"
	} else {
		sslmode = "https"
	}

	client := graphql.NewClient(fmt.Sprintf("%s%s:%s/graphql", sslmode, hostname, graphqlPort))
	dqlGrpc, err := grpc.Dial(fmt.Sprintf("%s:%s", hostname, dqlPort), grpc.WithInsecure())
	if err != nil {
		log.Println(err.Error())
	}

	dql := dgo.NewDgraphClient(
		api.NewDgraphClient(dqlGrpc),
	)

	return WithInstance(NewDB(client, dql), &Config{
		GraphQL: conformGraphql,
	})
}

func (d *DGraph) Close() error {
	return nil
}

func (d *DGraph) Lock() error {
	return nil
}

func (d *DGraph) Unlock() error {
	return nil
}

func (d *DGraph) Run(migration io.Reader) error {
	ctx := context.Background()

	decoder := json.NewDecoder(migration)

	var operations Operations
	err := decoder.Decode(&operations)
	if err != nil {
		fmt.Println(err.Error())
		return &database.Error{OrigErr: err, Err: "migration failed! could not parse operations"}
	}

	for _, alter := range operations.Alter {
		operation := alter.ToOperation()
		err := d.db.dql.Alter(ctx, operation)
		if err != nil {
			return &database.Error{OrigErr: err, Err: "migration failed! Reset to previous schema manually", Query: []byte(operation.String())}
		}
	}

	txn := d.db.dql.NewTxn()
	for _, mutationOp := range operations.Mutation {
		mutation, err := mutationOp.ToMutation()
		if err != nil {
			return &database.Error{OrigErr: err, Err: "migration failed - json malformed"}
		}

		_, err = txn.Mutate(ctx, mutation)
		if err != nil {
			return &database.Error{OrigErr: err, Err: "migration failed", Query: []byte(mutation.String())}
		}
	}
	for _, upsert := range operations.Upsert {
		req := &api.Request{}
		req.Query = upsert.Query
		for _, mutationOp := range upsert.Mutations {
			mutation, err := mutationOp.ToMutation()
			if err != nil {
				return &database.Error{OrigErr: err, Err: "migration failed - json malformed"}
			}

			req.Mutations = append(req.Mutations, mutation)
		}
		_, err := txn.Do(ctx, req)
		if err != nil {
			return &database.Error{OrigErr: err, Err: "migration failed", Query: []byte(req.String())}
		}
	}

	err = txn.Commit(ctx)
	if err != nil {
		return &database.Error{OrigErr: err, Err: "migration failed"}
	}
	return nil
}

func (d *DGraph) SetVersion(version int, dirty bool) error {
	ctx := context.Background()

	migration := &Migration{
		Uid:     "uid(v)",
		Version: version,
		Dirty:   dirty,
		Type:    "Migration",
	}

	var migrationJson []byte
	var err error
	if d.config.GraphQL {
		migrationJson, err = GraphQLJson.Marshal(migration)
	} else {
		migrationJson, err = DqlJson.Marshal(migration)
	}
	if err != nil {
		return &database.Error{OrigErr: err}
	}
	fmt.Println(string(migrationJson))

	mu := &api.Mutation{
		SetJson: migrationJson,
	}

	res, err := d.db.dql.NewTxn().Do(ctx, &api.Request{
		Query: `query {
			q(func: type(Migration)) {
				v as uid
		} }`,
		Mutations: []*api.Mutation{mu},
		CommitNow: true,
	})
	if err != nil {
		return &database.Error{OrigErr: err}
	}
	fmt.Println(string(res.GetJson()))

	return nil
}

func (d *DGraph) Version() (version int, dirty bool, err error) {
	ctx := context.Background()

	var res *api.Response

	if d.config.GraphQL {
		res, err = d.db.dql.NewReadOnlyTxn().Query(ctx, GetVersionGraphQL)
	} else {
		res, err = d.db.dql.NewReadOnlyTxn().Query(ctx, GetVersionDQL)
	}
	if err != nil {
		if d.config.GraphQL {
			return 0, false, &database.Error{OrigErr: err, Query: []byte(GetVersionGraphQL)}
		}
		return 0, false, &database.Error{OrigErr: err, Query: []byte(GetVersionDQL)}
	}

	var migrationResult struct {
		Migration []*Migration `graphql:"getVersion" dql:"getVersion"`
	}
	if d.config.GraphQL {
		err = GraphQLJson.Unmarshal(res.GetJson(), &migrationResult)
	} else {
		err = DqlJson.Unmarshal(res.GetJson(), &migrationResult)
	}
	if err != nil {
		return 0, false, &database.Error{OrigErr: err}
	}
	if len(migrationResult.Migration) == 0 {
		return database.NilVersion, false, nil
	}
	return migrationResult.Migration[0].Version, migrationResult.Migration[0].Dirty, nil
}

func (d *DGraph) Drop() error {
	return d.db.dql.Alter(context.Background(), &api.Operation{DropAll: true})
}
