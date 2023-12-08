package mongodb

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"strconv"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/golang-migrate/migrate/v4/database"
	"github.com/hashicorp/go-multierror"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/x/mongo/driver/connstring"
	"go.uber.org/atomic"
)

func init() {
	db := Mongo{}
	database.Register("mongodb", &db)
	database.Register("mongodb+srv", &db)
}

var DefaultMigrationsCollection = "schema_migrations"

const DefaultLockingCollection = "migrate_advisory_lock" // the collection to use for advisory locking by default.
const lockKeyUniqueValue = 0                             // the unique value to lock on. If multiple clients try to insert the same key, it will fail (locked).
const DefaultLockTimeout = 15                            // the default maximum time to wait for a lock to be released.
const DefaultLockTimeoutInterval = 10                    // the default maximum intervals time for the locking timout.
const DefaultAdvisoryLockingFlag = true                  // the default value for the advisory locking feature flag. Default is true.
const LockIndexName = "lock_unique_key"                  // the name of the index which adds unique constraint to the locking_key field.
const contextWaitTimeout = 5 * time.Second               // how long to wait for the request to mongo to block/wait for.

var (
	ErrNoDatabaseName            = fmt.Errorf("no database name")
	ErrNilConfig                 = fmt.Errorf("no config")
	ErrLockTimeoutConfigConflict = fmt.Errorf("both x-advisory-lock-timeout-interval and x-advisory-lock-timout-interval were specified")
)

type Mongo struct {
	client   *mongo.Client
	db       *mongo.Database
	config   *Config
	isLocked atomic.Bool
}

type Locking struct {
	CollectionName string
	Timeout        int
	Enabled        bool
	Interval       int
}
type Config struct {
	DatabaseName         string
	MigrationsCollection string
	TransactionMode      bool
	Locking              Locking
}
type versionInfo struct {
	Version int  `bson:"version"`
	Dirty   bool `bson:"dirty"`
}

type lockObj struct {
	Key       int       `bson:"locking_key"`
	Pid       int       `bson:"pid"`
	Hostname  string    `bson:"hostname"`
	CreatedAt time.Time `bson:"created_at"`
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
	if len(config.Locking.CollectionName) == 0 {
		config.Locking.CollectionName = DefaultLockingCollection
	}
	if config.Locking.Timeout <= 0 {
		config.Locking.Timeout = DefaultLockTimeout
	}
	if config.Locking.Interval <= 0 {
		config.Locking.Interval = DefaultLockTimeoutInterval
	}

	mc := &Mongo{
		client: instance,
		db:     instance.Database(config.DatabaseName),
		config: config,
	}

	if mc.config.Locking.Enabled {
		if err := mc.ensureLockTable(); err != nil {
			return nil, err
		}
	}
	if err := mc.ensureVersionTable(); err != nil {
		return nil, err
	}

	return mc, nil
}

func (m *Mongo) Open(dsn string) (database.Driver, error) {
	// connstring is experimental package, but it used for parse connection string in mongo.Connect function
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
	transactionMode, err := parseBoolean(unknown.Get("x-transaction-mode"), false)
	if err != nil {
		return nil, err
	}
	advisoryLockingFlag, err := parseBoolean(unknown.Get("x-advisory-locking"), DefaultAdvisoryLockingFlag)
	if err != nil {
		return nil, err
	}
	lockingTimout, err := parseInt(unknown.Get("x-advisory-lock-timeout"), DefaultLockTimeout)
	if err != nil {
		return nil, err
	}

	lockTimeoutIntervalValue := unknown.Get("x-advisory-lock-timeout-interval")
	// The initial release had a typo for this argument but for backwards compatibility sake, we will keep supporting it
	// and we will error out if both values are set.
	lockTimeoutIntervalValueFromTypo := unknown.Get("x-advisory-lock-timout-interval")

	lockTimeout := lockTimeoutIntervalValue

	if lockTimeoutIntervalValue != "" && lockTimeoutIntervalValueFromTypo != "" {
		return nil, ErrLockTimeoutConfigConflict
	} else if lockTimeoutIntervalValueFromTypo != "" {
		lockTimeout = lockTimeoutIntervalValueFromTypo
	}

	maxLockCheckInterval, err := parseInt(lockTimeout, DefaultLockTimeoutInterval)

	if err != nil {
		return nil, err
	}
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
		TransactionMode:      transactionMode,
		Locking: Locking{
			CollectionName: lockCollection,
			Timeout:        lockingTimout,
			Enabled:        advisoryLockingFlag,
			Interval:       maxLockCheckInterval,
		},
	})
	if err != nil {
		return nil, err
	}
	return mc, nil
}

// Parse the url param, convert it to boolean
// returns error if param invalid. returns defaultValue if param not present
func parseBoolean(urlParam string, defaultValue bool) (bool, error) {

	// if parameter passed, parse it (otherwise return default value)
	if urlParam != "" {
		result, err := strconv.ParseBool(urlParam)
		if err != nil {
			return false, err
		}
		return result, nil
	}

	// if no url Param passed, return default value
	return defaultValue, nil
}

// Parse the url param, convert it to int
// returns error if param invalid. returns defaultValue if param not present
func parseInt(urlParam string, defaultValue int) (int, error) {

	// if parameter passed, parse it (otherwise return default value)
	if urlParam != "" {
		result, err := strconv.Atoi(urlParam)
		if err != nil {
			return -1, err
		}
		return result, nil
	}

	// if no url Param passed, return default value
	return defaultValue, nil
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
	migr, err := io.ReadAll(migration)
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
			// When command execution is failed, it's aborting transaction
			// If you tried to call abortTransaction, it`s return error that transaction already aborted
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

func (m *Mongo) ensureLockTable() error {
	indexes := m.db.Collection(m.config.Locking.CollectionName).Indexes()

	indexOptions := options.Index().SetUnique(true).SetName(LockIndexName)
	_, err := indexes.CreateOne(context.TODO(), mongo.IndexModel{
		Options: indexOptions,
		Keys:    findFilter{Key: -1},
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
	return database.CasRestoreOnErr(&m.isLocked, false, true, database.ErrLocked, func() error {
		if !m.config.Locking.Enabled {
			return nil
		}

		pid := os.Getpid()
		hostname, err := os.Hostname()
		if err != nil {
			hostname = fmt.Sprintf("Could not determine hostname. Error: %s", err.Error())
		}

		newLockObj := lockObj{
			Key:       lockKeyUniqueValue,
			Pid:       pid,
			Hostname:  hostname,
			CreatedAt: time.Now(),
		}
		operation := func() error {
			timeout, cancelFunc := context.WithTimeout(context.Background(), contextWaitTimeout)
			_, err := m.db.Collection(m.config.Locking.CollectionName).InsertOne(timeout, newLockObj)
			defer cancelFunc()
			return err
		}
		exponentialBackOff := backoff.NewExponentialBackOff()
		duration := time.Duration(m.config.Locking.Timeout) * time.Second
		exponentialBackOff.MaxElapsedTime = duration
		exponentialBackOff.MaxInterval = time.Duration(m.config.Locking.Interval) * time.Second

		err = backoff.Retry(operation, exponentialBackOff)
		if err != nil {
			return database.ErrLocked
		}

		return nil
	})
}

func (m *Mongo) Unlock() error {
	return database.CasRestoreOnErr(&m.isLocked, true, false, database.ErrNotLocked, func() error {
		if !m.config.Locking.Enabled {
			return nil
		}

		filter := findFilter{
			Key: lockKeyUniqueValue,
		}

		ctx, cancel := context.WithTimeout(context.Background(), contextWaitTimeout)
		_, err := m.db.Collection(m.config.Locking.CollectionName).DeleteMany(ctx, filter)
		defer cancel()

		if err != nil {
			return err
		}
		return nil
	})
}
