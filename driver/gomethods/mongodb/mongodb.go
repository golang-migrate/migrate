package mongodb

import (
	"errors"
	"github.com/dimag-jfrog/migrate/driver"
	"github.com/dimag-jfrog/migrate/driver/gomethods"
	"github.com/dimag-jfrog/migrate/file"
	"github.com/dimag-jfrog/migrate/migrate/direction"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"reflect"
	"strings"
)

const MIGRATE_DB = "db_migrations"
const MIGRATE_C = "db_migrations"

type MongoDbGoMethodsDriver struct {
	Session *mgo.Session
	DbName  string

	migrator gomethods.Migrator
}

func init() {
	driver.RegisterDriver("mongodb", &MongoDbGoMethodsDriver{})
}

type DbMigration struct {
	Id      bson.ObjectId `bson:"_id,omitempty"`
	Version uint64        `bson:"version"`
}

func (driver *MongoDbGoMethodsDriver) Initialize(url string) error {
	urlWithoutScheme := strings.SplitN(url, "mongodb://", 2)
	if len(urlWithoutScheme) != 2 {
		return errors.New("invalid mongodb:// scheme")
	}

	session, err := mgo.Dial(url)
	if err != nil {
		return err
	}
	session.SetMode(mgo.Monotonic, true)

	driver.Session = session
	driver.DbName = MIGRATE_DB
	driver.migrator = gomethods.Migrator{MethodInvoker: driver}

	return nil
}

func (driver *MongoDbGoMethodsDriver) Close() error {
	if driver.Session != nil {
		driver.Session.Close()
	}
	return nil
}

func (driver *MongoDbGoMethodsDriver) FilenameExtension() string {
	return "mgo"
}

func (driver *MongoDbGoMethodsDriver) Version() (uint64, error) {
	var latestMigration DbMigration
	c := driver.Session.DB(driver.DbName).C(MIGRATE_C)

	err := c.Find(bson.M{}).Sort("-version").One(&latestMigration)

	switch {
	case err == mgo.ErrNotFound:
		return 0, nil
	case err != nil:
		return 0, err
	default:
		return latestMigration.Version, nil
	}
}
func (driver *MongoDbGoMethodsDriver) Migrate(f file.File, pipe chan interface{}) {
	defer close(pipe)
	pipe <- f

	err := driver.migrator.Migrate(f, pipe)
	if err != nil {
		return
	}

	migrate_c := driver.Session.DB(driver.DbName).C(MIGRATE_C)

	if f.Direction == direction.Up {
		id := bson.NewObjectId()
		dbMigration := DbMigration{Id: id, Version: f.Version}

		err := migrate_c.Insert(dbMigration)
		if err != nil {
			pipe <- err
			return
		}

	} else if f.Direction == direction.Down {
		err := migrate_c.Remove(bson.M{"version": f.Version})
		if err != nil {
			pipe <- err
			return
		}
	}
}

func (driver *MongoDbGoMethodsDriver) IsValid(methodName string, methodsReceiver interface{}) bool {
	return reflect.ValueOf(methodsReceiver).MethodByName(methodName).IsValid()
}

func (driver *MongoDbGoMethodsDriver) Invoke(methodName string, methodsReceiver interface{}) error {
	name := methodName
	migrateMethod := reflect.ValueOf(methodsReceiver).MethodByName(name)
	if !migrateMethod.IsValid() {
		return gomethods.MissingMethodError(methodName)
	}

	retValues := migrateMethod.Call([]reflect.Value{reflect.ValueOf(driver.Session)})
	if len(retValues) != 1 {
		return gomethods.WrongMethodSignatureError(name)
	}

	if !retValues[0].IsNil() {
		err, ok := retValues[0].Interface().(error)
		if !ok {
			return gomethods.WrongMethodSignatureError(name)
		}
		return &gomethods.MethodInvocationFailedError{MethodName: name, Err: err}
	}

	return nil
}
