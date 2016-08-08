package usage_examples

import (
	"github.com/dimag-jfrog/migrate/driver"
	"github.com/dimag-jfrog/migrate/driver/gomethods"
	"github.com/dimag-jfrog/migrate/driver/gomethods/mongodb"
	"gopkg.in/mgo.v2/bson"
	"time"
)

// This boilerplate part is necessary and the same
// regardless of the specific mongodb golang methods driver

type GoMethodsMongoDbDriver struct {
	mongodb.DriverTemplate
}

func (d *GoMethodsMongoDbDriver) Initialize(url string) error {
	return d.DriverTemplate.Initialize(url, DB_NAME, gomethods.Migrator{Driver: d})
}

func init() {
	driver.RegisterDriver("mongodb", &GoMethodsMongoDbDriver{mongodb.DriverTemplate{}})
}



// Here goes the specific mongodb golang methods driver logic

const DB_NAME = "test"
const SHORT_DATE_LAYOUT = "2000-Jan-01"
const USERS_C = "users"
const ORGANIZATIONS_C = "organizations"

type Organization struct {
	Id                bson.ObjectId          `bson:"_id,omitempty"`
	Name              string                 `bson:"name"`
	Location          string                 `bson:"location"`
	DateFounded       time.Time              `bson:"date_founded"`
}

type Organization_v2 struct {
	Id                bson.ObjectId          `bson:"_id,omitempty"`
	Name              string                 `bson:"name"`
	Headquarters      string                 `bson:"headquarters"`
	DateFounded       time.Time              `bson:"date_founded"`
}

type User struct {
	Id          bson.ObjectId          `bson:"_id"`
	Name        string                 `bson:"name"`
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

func (m *GoMethodsMongoDbDriver) V001_init_organizations_up() error {
	date1, _ := time.Parse(SHORT_DATE_LAYOUT, "1994-Jul-05")
	date2, _ := time.Parse(SHORT_DATE_LAYOUT, "1998-Sep-04")
	date3, _ := time.Parse(SHORT_DATE_LAYOUT, "2008-Apr-28")

	orgs := []Organization{
		{Id: OrganizationIds[0], Name: "Amazon", Location:"Seattle", DateFounded: date1},
		{Id: OrganizationIds[1], Name: "Google", Location:"Mountain View", DateFounded: date2},
		{Id: OrganizationIds[2], Name: "JFrog", Location:"Santa Clara", DateFounded: date3},
	}

	for _, org := range orgs {
		err := m.Session.DB(DB_NAME).C(ORGANIZATIONS_C).Insert(org)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *GoMethodsMongoDbDriver) V001_init_organizations_down() error {
	return m.Session.DB(DB_NAME).C(ORGANIZATIONS_C).DropCollection()
}

func (m *GoMethodsMongoDbDriver) V001_init_users_up() error {
	users := []User{
		{Id: UserIds[0], Name: "Alex"},
		{Id: UserIds[1], Name: "Beatrice"},
		{Id: UserIds[2], Name: "Cleo"},
	}

	for _, user := range users {
		err := m.Session.DB(DB_NAME).C(USERS_C).Insert(user)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *GoMethodsMongoDbDriver) V001_init_users_down() error {
	return m.Session.DB(DB_NAME).C(USERS_C).DropCollection()
}

func (m *GoMethodsMongoDbDriver) V002_organizations_rename_location_field_to_headquarters_up() error {
	c := m.Session.DB(DB_NAME).C(ORGANIZATIONS_C)

	_, err := c.UpdateAll(nil, bson.M{"$rename": bson.M{"location": "headquarters"}})
	return err
}

func (m *GoMethodsMongoDbDriver) V002_organizations_rename_location_field_to_headquarters_down() error {
	c := m.Session.DB(DB_NAME).C(ORGANIZATIONS_C)

	_, err := c.UpdateAll(nil, bson.M{"$rename": bson.M{"headquarters": "location"}})
	return err
}

func (m *GoMethodsMongoDbDriver) V002_change_user_cleo_to_cleopatra_up() error {
	c := m.Session.DB(DB_NAME).C(USERS_C)

	colQuerier := bson.M{"name": "Cleo"}
	change := bson.M{"$set": bson.M{"name": "Cleopatra"}}

	return c.Update(colQuerier, change)
}

func (m *GoMethodsMongoDbDriver) V002_change_user_cleo_to_cleopatra_down() error {
	c := m.Session.DB(DB_NAME).C(USERS_C)

	colQuerier := bson.M{"name": "Cleopatra"}
	change := bson.M{"$set": bson.M{"name": "Cleo",}}

	return c.Update(colQuerier, change)
}