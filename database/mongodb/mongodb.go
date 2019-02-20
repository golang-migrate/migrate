package mongodb

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"strconv"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/mongodb/mongo-go-driver/mongo"
	"github.com/mongodb/mongo-go-driver/x/network/connstring"
)

func init() {
	database.Register("mongodb", &Mongo{})
}

var DefaultMigrationsCollection = "schema_migrations"

var (
	ErrNoDatabaseName = fmt.Errorf("no database name")
	ErrNilConfig      = fmt.Errorf("no config")
)

type Mongo struct {
	client *mongo.Client
	db     *mongo.Database

	config *Config
}

type Config struct {
	DatabaseName         string
	MigrationsCollection string
	TransactionMode      bool
}

type versionInfo struct {
	Version int  `bson:"version"`
	Dirty   bool `bson:"dirty"`
}

func WithInstance(instance *mongo.Client, config *Config) (database.Driver, error) {
	if config == nil {
		return nil, ErrNilConfig
	}
	if len(config.DatabaseName) == 0 {
		return nil, ErrNoDatabaseName
	}
	if len(config.MigrationsCollection) == 0 {
		config.MigrationsCollection = DefaultMigrationsCollection
	}
	mc := &Mongo{
		client: instance,
		db:     instance.Database(config.DatabaseName),
		config: config,
	}

	return mc, nil
}

func (m *Mongo) Open(dsn string) (database.Driver, error) {
	//connsting is experimental package, but it used for parse connection string in mongo.Connect function
	uri, err := connstring.Parse(dsn)
	if err != nil {
		return nil, err
	}
	if len(uri.Database) == 0 {
		return nil, ErrNoDatabaseName
	}

	purl, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}
	migrationsCollection := purl.Query().Get("x-migrations-collection")

	transactionMode, _ := strconv.ParseBool(purl.Query().Get("x-transaction-mode"))

	q := migrate.FilterCustomQuery(purl)
	q.Scheme = "mongodb"

	client, err := mongo.Connect(context.TODO(), q.String())
	if err != nil {
		return nil, err
	}
	if err = client.Ping(context.TODO(), nil); err != nil {
		return nil, err
	}
	mc, err := WithInstance(client, &Config{
		DatabaseName:         uri.Database,
		MigrationsCollection: migrationsCollection,
		TransactionMode:      transactionMode,
	})
	if err != nil {
		return nil, err
	}
	return mc, nil
}

func (m *Mongo) SetVersion(version int, dirty bool) error {
	migrationsCollection := m.db.Collection(m.config.MigrationsCollection)
	if err := migrationsCollection.Drop(context.TODO()); err != nil {
		return &database.Error{OrigErr: err, Err: "drop migrations collection failed"}
	}
	_, err := migrationsCollection.InsertOne(context.TODO(), bson.M{"version": version, "dirty": dirty})
	if err != nil {
		return &database.Error{OrigErr: err, Err: "save version failed"}
	}
	return nil
}

func (m *Mongo) Version() (version int, dirty bool, err error) {
	var versionInfo versionInfo
	err = m.db.Collection(m.config.MigrationsCollection).FindOne(context.TODO(), bson.M{}).Decode(&versionInfo)
	switch {
	case err == mongo.ErrNoDocuments:
		return database.NilVersion, false, nil
	case err != nil:
		return 0, false, &database.Error{OrigErr: err, Err: "failed to get migration version"}
	default:
		return versionInfo.Version, versionInfo.Dirty, nil
	}
}

func (m *Mongo) Run(migration io.Reader) error {
	migr, err := ioutil.ReadAll(migration)
	if err != nil {
		return err
	}
	var cmds []bson.D
	err = bson.UnmarshalExtJSON(migr, true, &cmds)
	if err != nil {
		return fmt.Errorf("unmarshaling json error: %s", err)
	}
	if m.config.TransactionMode {
		if err := m.executeCommandsWithTransaction(context.TODO(), cmds); err != nil {
			return err
		}
	} else {
		if err := m.executeCommands(context.TODO(), cmds); err != nil {
			return err
		}
	}
	return nil
}

func (m *Mongo) executeCommandsWithTransaction(ctx context.Context, cmds []bson.D) error {
	err := m.db.Client().UseSession(ctx, func(sessionContext mongo.SessionContext) error {
		if err := sessionContext.StartTransaction(); err != nil {
			return &database.Error{OrigErr: err, Err: "failed to start transaction"}
		}
		if err := m.executeCommands(sessionContext, cmds); err != nil {
			//When command execution is failed, it's aborting transaction
			//If you tried to call abortTransaction, it`s return error that transaction already aborted
			return err
		}
		if err := sessionContext.CommitTransaction(sessionContext); err != nil {
			return &database.Error{OrigErr: err, Err: "failed to commit transaction"}
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

func (m *Mongo) executeCommands(ctx context.Context, cmds []bson.D) error {
	for _, cmd := range cmds {
		err := m.db.RunCommand(ctx, cmd).Err()
		if err != nil {
			return &database.Error{OrigErr: err, Err: fmt.Sprintf("failed to execute command:%v", cmd)}
		}
	}
	return nil
}

func (m *Mongo) Close() error {
	return m.client.Disconnect(context.TODO())
}

func (m *Mongo) Drop() error {
	return m.db.Drop(context.TODO())
}

func (m *Mongo) Lock() error {
	return nil
}

func (m *Mongo) Unlock() error {
	return nil
}
