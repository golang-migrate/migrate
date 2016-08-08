package usage_examples

import (
	"testing"

	"github.com/dimag-jfrog/migrate/file"
	"github.com/dimag-jfrog/migrate/migrate/direction"

	pipep "github.com/dimag-jfrog/migrate/pipe"
)



func TestMigrate(t *testing.T) {
	//host := os.Getenv("MONGODB_PORT_27017_TCP_ADDR")
	//port := os.Getenv("MONGODB_PORT_27017_TCP_PORT")
	host := "127.0.0.1"
	port := "27017"
	driverUrl := "mongodb://" + host + ":" + port

	d := &GoMethodsMongoDbDriver{}
	if err := d.Initialize(driverUrl); err != nil {
		t.Fatal(err)
	}

	content1 := []byte(`
				V001_init_organizations
				V001_init_users
			`)
	content2 := []byte(`
				V002_organizations_rename_location_field_to_headquarters
				V002_change_user_cleo_to_cleopatra
			`)

	files := []file.File{
		{
			Path:      "/foobar",
			FileName:  "001_foobar.mgo",
			Version:   1,
			Name:      "foobar",
			Direction: direction.Up,
			Content: content1,
		},
		{
			Path:      "/foobar",
			FileName:  "001_foobar.mgo",
			Version:   1,
			Name:      "foobar",
			Direction: direction.Down,
			Content: content1,
		},
		{
			Path:      "/foobar",
			FileName:  "002_foobar.mgo",
			Version:   2,
			Name:      "foobar",
			Direction: direction.Up,
			Content: content2,
		},
		{
			Path:      "/foobar",
			FileName:  "002_foobar.mgo",
			Version:   2,
			Name:      "foobar",
			Direction: direction.Down,
			Content: content2,
		},
		{
			Path:      "/foobar",
			FileName:  "001_foobar.mgo",
			Version:   1,
			Name:      "foobar",
			Direction: direction.Up,
			Content: []byte(`
				V001_init_organizations
				V001_init_users
				V001_non_existing_operation
			`),
		},
	}

	var pipe chan interface{}
	var errs []error

	pipe = pipep.New()
	go d.Migrate(files[0], pipe)
	errs = pipep.ReadErrors(pipe)
	if len(errs) > 0 {
		t.Fatal(errs)
	}

	pipe = pipep.New()
	go d.Migrate(files[2], pipe)
	errs = pipep.ReadErrors(pipe)
	if len(errs) > 0 {
		t.Fatal(errs)
	}

	pipe = pipep.New()
	go d.Migrate(files[3], pipe)
	errs = pipep.ReadErrors(pipe)
	if len(errs) > 0 {
		t.Fatal(errs)
	}

	//pipe = pipep.New()
	//go d.Migrate(files[1], pipe)
	//errs = pipep.ReadErrors(pipe)
	//if len(errs) > 0 {
	//	t.Fatal(errs)
	//}

	pipe = pipep.New()
	go d.Migrate(files[4], pipe)
	errs = pipep.ReadErrors(pipe)
	if len(errs) == 0 {
		t.Error("Expected test case to fail")
	}

	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
}
