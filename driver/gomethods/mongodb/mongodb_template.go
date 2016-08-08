package mongodb

import (
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"errors"
	"strings"
	"github.com/dimag-jfrog/migrate/migrate/direction"
	"github.com/dimag-jfrog/migrate/file"
	"github.com/dimag-jfrog/migrate/driver/gomethods"
)

const MIGRATE_C = "db_migrations"


// This is not a real driver since the Initialize method requires a gomethods.Migrator
// The real driver will contain the DriverTemplate and implement all the custom migration Golang methods
// See example in usage_examples for details
type DriverTemplate struct {
	Session *mgo.Session
	DbName string

	migrator gomethods.Migrator
}


type DbMigration struct {
	Id             bson.ObjectId   `bson:"_id,omitempty"`
	Version        uint64          `bson:"version"`
}

func (driver *DriverTemplate) Initialize(url, dbName string, migrator gomethods.Migrator) error {
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
	driver.DbName = dbName
	driver.migrator = migrator

	return nil
}

func (driver *DriverTemplate) Close() error {
	if driver.Session != nil {
		driver.Session.Close()
	}
	return nil
}

func (driver *DriverTemplate) FilenameParser() file.FilenameParser {
	return file.UpDownAndBothFilenameParser{FilenameExtension: driver.FilenameExtension()}
}

func (driver *DriverTemplate) FilenameExtension() string {
	return "mgo"
}


func (driver *DriverTemplate) Version() (uint64, error) {
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
func (driver *DriverTemplate) Migrate(f file.File, pipe chan interface{}) {
	defer close(pipe)
	pipe <- f

	err := driver.migrator.Migrate(f, pipe)
	if err != nil {
		return
	}

	migrate_c :=  driver.Session.DB(driver.DbName).C(MIGRATE_C)

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
