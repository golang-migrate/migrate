package example

import (
	"github.com/mattes/migrate/driver/mongodb/gomethods"
	_ "github.com/mattes/migrate/driver/mongodb/gomethods"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"time"

	"github.com/mattes/migrate/driver/mongodb"
)

type SampleMongoDbMigrator struct {
}

func (r *SampleMongoDbMigrator) DbName() string {
	return DB_NAME
}

var _ mongodb.MethodsReceiver = (*SampleMongoDbMigrator)(nil)

func init() {
	gomethods.RegisterMethodsReceiverForDriver("mongodb", &SampleMongoDbMigrator{})
}

// Here goes the specific mongodb golang methods migration logic

const (
	DB_NAME           = "test"
	SHORT_DATE_LAYOUT = "2000-Jan-01"
	USERS_C           = "users"
	ORGANIZATIONS_C   = "organizations"
)

type Organization struct {
	Id          bson.ObjectId `bson:"_id,omitempty"`
	Name        string        `bson:"name"`
	Location    string        `bson:"location"`
	DateFounded time.Time     `bson:"date_founded"`
}

type Organization_v2 struct {
	Id           bson.ObjectId `bson:"_id,omitempty"`
	Name         string        `bson:"name"`
	Headquarters string        `bson:"headquarters"`
	DateFounded  time.Time     `bson:"date_founded"`
}

type User struct {
	Id   bson.ObjectId `bson:"_id"`
	Name string        `bson:"name"`
}

var OrganizationIds []bson.ObjectId = []bson.ObjectId{
	bson.NewObjectId(),
	bson.NewObjectId(),
	bson.NewObjectId(),
}

var UserIds []bson.ObjectId = []bson.ObjectId{
	bson.NewObjectId(),
	bson.NewObjectId(),
	bson.NewObjectId(),
}

func (r *SampleMongoDbMigrator) V001_init_organizations_up(session *mgo.Session) error {
	date1, _ := time.Parse(SHORT_DATE_LAYOUT, "1994-Jul-05")
	date2, _ := time.Parse(SHORT_DATE_LAYOUT, "1998-Sep-04")
	date3, _ := time.Parse(SHORT_DATE_LAYOUT, "2008-Apr-28")

	orgs := []Organization{
		{Id: OrganizationIds[0], Name: "Amazon", Location: "Seattle", DateFounded: date1},
		{Id: OrganizationIds[1], Name: "Google", Location: "Mountain View", DateFounded: date2},
		{Id: OrganizationIds[2], Name: "JFrog", Location: "Santa Clara", DateFounded: date3},
	}

	for _, org := range orgs {
		err := session.DB(DB_NAME).C(ORGANIZATIONS_C).Insert(org)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *SampleMongoDbMigrator) V001_init_organizations_down(session *mgo.Session) error {
	return session.DB(DB_NAME).C(ORGANIZATIONS_C).DropCollection()
}

func (r *SampleMongoDbMigrator) V001_init_users_up(session *mgo.Session) error {
	users := []User{
		{Id: UserIds[0], Name: "Alex"},
		{Id: UserIds[1], Name: "Beatrice"},
		{Id: UserIds[2], Name: "Cleo"},
	}

	for _, user := range users {
		err := session.DB(DB_NAME).C(USERS_C).Insert(user)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *SampleMongoDbMigrator) V001_init_users_down(session *mgo.Session) error {
	return session.DB(DB_NAME).C(USERS_C).DropCollection()
}

func (r *SampleMongoDbMigrator) V002_organizations_rename_location_field_to_headquarters_up(session *mgo.Session) error {
	c := session.DB(DB_NAME).C(ORGANIZATIONS_C)

	_, err := c.UpdateAll(nil, bson.M{"$rename": bson.M{"location": "headquarters"}})
	return err
}

func (r *SampleMongoDbMigrator) V002_organizations_rename_location_field_to_headquarters_down(session *mgo.Session) error {
	c := session.DB(DB_NAME).C(ORGANIZATIONS_C)

	_, err := c.UpdateAll(nil, bson.M{"$rename": bson.M{"headquarters": "location"}})
	return err
}

func (r *SampleMongoDbMigrator) V002_change_user_cleo_to_cleopatra_up(session *mgo.Session) error {
	c := session.DB(DB_NAME).C(USERS_C)

	colQuerier := bson.M{"name": "Cleo"}
	change := bson.M{"$set": bson.M{"name": "Cleopatra"}}

	return c.Update(colQuerier, change)
}

func (r *SampleMongoDbMigrator) V002_change_user_cleo_to_cleopatra_down(session *mgo.Session) error {
	c := session.DB(DB_NAME).C(USERS_C)

	colQuerier := bson.M{"name": "Cleopatra"}
	change := bson.M{"$set": bson.M{"name": "Cleo"}}

	return c.Update(colQuerier, change)
}

// Wrong signature methods for testing
func (r *SampleMongoDbMigrator) v001_not_exported_method_up(session *mgo.Session) error {
	return nil
}

func (r *SampleMongoDbMigrator) V001_method_with_wrong_signature_up(s string) error {
	return nil
}

func (r *SampleMongoDbMigrator) V001_method_with_wrong_signature_down(session *mgo.Session) (bool, error) {
	return true, nil
}
