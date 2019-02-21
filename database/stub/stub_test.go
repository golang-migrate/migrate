package stub

import (
	"github.com/golang-migrate/migrate/v4"
	"testing"

	dt "github.com/golang-migrate/migrate/v4/database/testing"
)

func Test(t *testing.T) {
	s := &Stub{}
	d, err := s.Open("")
	if err != nil {
		t.Fatal(err)
	}
	dt.Test(t, d, []byte("/* foobar migration */"))
}

func TestMigrate(t *testing.T) {
	s := &Stub{}
	d, err := s.Open("")
	if err != nil {
		t.Fatal(err)
	}

	m, err := migrate.NewWithDatabaseInstance("stub://", "", d)
	if err != nil {
		t.Fatalf("%v", err)
	}
	dt.TestMigrate(t, m, []byte("/* foobar migration */"))
}
