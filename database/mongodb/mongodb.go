package mongodb

import (
	"context"
	"fmt"
	"github.com/cenkalti/backoff/v4"
	"github.com/golang-migrate/migrate/v4/database"
	"github.com/hashicorp/go-multierror"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/x/bsonx"
	"go.mongodb.org/mongo-driver/x/mongo/driver/connstring"
	"io"
	"io/ioutil"
	"net/url"
	os "os"
	"strconv"
	"time"
)

func init() {
	db := Mongo{}
	database.Register("mongodb", &db)
	database.Register("mongodb+srv", &db)
}

var DefaultMigrationsCollection = "schema_migrations"

const DefaultLockingCollection = "migrate_advisory_lock" // the collection to use for advisory locking by default.
const LockingKey = "locking_key"                         // the key to lock on, will have a unique=true index on it
const lockKeyUniqueValue = 0                             // the unique value to lock on. If multiple clients try to insert the same key, it will fail (locked).
const LockingBackoffTime = 15                            // the default maximum time to wait for a lock to be released

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
	LockingCollection    string
	LockingBackoffTime   int
	TransactionMode      bool
}

type versionInfo struct {
	Version int  `bson:"version"`
	Dirty   bool `bson:"dirty"`
}

type lockObj struct {
	Key  int    `bson:"locking_key"`
	Pid  int    `bson:"pid"`
	Name string `bson:"hostname"`
}
type findFilter struct {
	Key int `bson:"locking_key"`
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
	if len(config.LockingCollection) == 0 {
		config.LockingCollection = DefaultLockingCollection
	}
	if config.LockingBackoffTime <= 0 {
		config.LockingBackoffTime = LockingBackoffTime
	}

	mc := &Mongo{
		client: instance,
		db:     instance.Database(config.DatabaseName),
		config: config,
	}

	if err := mc.ensureLockTable(); err != nil {
		return nil, err
	}
	if err := mc.ensureVersionTable(); err != nil {
		return nil, err
	}

	return mc, nil
}

func (m *Mongo) Open(dsn string) (database.Driver, error) {
	//connstring is experimental package, but it used for parse connection string in mongo.Connect function
	uri, err := connstring.Parse(dsn)
	if err != nil {
		return nil, err
	}
	if len(uri.Database) == 0 {
		return nil, ErrNoDatabaseName
	}
	unknown := url.Values(uri.UnknownOptions)

	migrationsCollection := unknown.Get("x-migrations-collection")
	lockCollection := unknown.Get("x-advisory-lock-collection")
	lockingBackoffTime, _ := strconv.Atoi(unknown.Get("x-advisory-lock-backoff-seconds"))
	transactionMode, _ := strconv.ParseBool(unknown.Get("x-transaction-mode"))

	client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(dsn))
	if err != nil {
		return nil, err
	}
	if err = client.Ping(context.TODO(), nil); err != nil {
		return nil, err
	}
	mc, err := WithInstance(client, &Config{
		DatabaseName:         uri.Database,
		MigrationsCollection: migrationsCollection,
		LockingCollection:    lockCollection,
		LockingBackoffTime:   lockingBackoffTime,
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

// Note that this could possibly have a race condition
// if three migrate processes try to create the index at the exact same time, but it
// takes a while for the first call to build the index (although it's an empty collection, so that may not take long)
// then two of them could return successful, while the index is still building, leading to
// the second and third processes to successfully insert a document (and "acquire" the lock),
// as duplicate keys would be allowed.
//
// This may not be an issue, if the collection is empty, and creating the lock takes next to no time.
//
func (m *Mongo) ensureLockTable() error {
	indexes := m.db.Collection(m.config.LockingCollection).Indexes()
	indexOptions := options.Index().SetUnique(true).SetName("lock_unique_key")
	indexKeys := bsonx.MDoc{
		LockingKey: bsonx.Int32(-1),
	}
	_, err := indexes.CreateOne(context.TODO(), mongo.IndexModel{
		Options: indexOptions,
		Keys:    indexKeys,
	})
	if err != nil {
		return err
	}
	return nil
}

// ensureVersionTable checks if versions table exists and, if not, creates it.
// Note that this function locks the database, which deviates from the usual
// convention of "caller locks" in the MongoDb type.
func (m *Mongo) ensureVersionTable() (err error) {
	if err = m.Lock(); err != nil {
		return err
	}

	defer func() {
		if e := m.Unlock(); e != nil {
			if err == nil {
				err = e
			} else {
				err = multierror.Append(err, e)
			}
		}
	}()

	if err != nil {
		return err
	}
	if _, _, err = m.Version(); err != nil {
		return err
	}
	return nil
}

// Utilizes advisory locking on the config.LockingCollection collection
// This uses a unique index on the `locking_key` field.
func (m *Mongo) Lock() error {
	pid := os.Getpid()
	hostname, err := os.Hostname()
	if err != nil {
		hostname = fmt.Sprintf("Could not determine hostname. Error: %s", err.Error())
	}

	newLockObj := lockObj{
		Key:  lockKeyUniqueValue,
		Pid:  pid,
		Name: hostname,
	}
	operation := func() error {
		_, err := m.db.Collection(m.config.LockingCollection).InsertOne(context.TODO(), newLockObj)
		return err
	}
	exponentialBackOff := backoff.NewExponentialBackOff()
	duration := time.Duration(m.config.LockingBackoffTime) * time.Second
	exponentialBackOff.MaxElapsedTime = duration
	exponentialBackOff.MaxInterval = exponentialBackOff.MaxElapsedTime / 10

	err = backoff.Retry(operation, exponentialBackOff)
	if err != nil {
		return database.ErrLocked
	}
	return nil

}
func (m *Mongo) Unlock() error {

	filter := findFilter{
		Key: lockKeyUniqueValue,
	}
	_, err := m.db.Collection(m.config.LockingCollection).DeleteMany(context.TODO(), filter)
	if err != nil {
		return err
	}
	return nil
}
