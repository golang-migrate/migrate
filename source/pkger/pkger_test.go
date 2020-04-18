package pkger

import (
	"log"
	"testing"

	"github.com/gobuffalo/here"
	st "github.com/golang-migrate/migrate/v4/source/testing"
	"github.com/markbates/pkger/pkging"
	"github.com/markbates/pkger/pkging/mem"
)

func Test(t *testing.T) {
	i := testInstance(t)

	d, err := WithInstance(i)
	if err != nil {
		t.Fatal(err)
	}
	st.Test(t, d)
}

func TestWithInstance(t *testing.T) {
	i := testInstance(t)

	_, err := WithInstance(i)
	if err != nil {
		t.Fatal(err)
	}
}

func TestOpen(t *testing.T) {
	d := &driver{}
	_, err := d.Open("")
	if err == nil {
		t.Fatal("expected err, because it's not implemented yet")
	}
}

func testInstance(t *testing.T) *Pkger {
	info, err := here.New().Current()
	if err != nil {
		t.Fatalf("failed to get the current here.Info: %v\n", err)
	}

	pkg, err := mem.New(info)
	if err != nil {
		log.Fatalf("failed to create an in-memory pkging.Pkger: %v\n", err)
	}

	createMigrationFile(t, pkg, "/1_foobar.up.sql")
	createMigrationFile(t, pkg, "/1_foobar.down.sql")
	createMigrationFile(t, pkg, "/3_foobar.up.sql")
	createMigrationFile(t, pkg, "/4_foobar.up.sql")
	createMigrationFile(t, pkg, "/4_foobar.down.sql")
	createMigrationFile(t, pkg, "/5_foobar.down.sql")
	createMigrationFile(t, pkg, "/7_foobar.up.sql")
	createMigrationFile(t, pkg, "/7_foobar.down.sql")

	return &Pkger{
		Pkger: pkg,
	}
}

func createMigrationFile(t *testing.T, pkg pkging.Pkger, m string) {
	_, err := pkg.Create(m)
	if err != nil {
		t.Fatalf("failed to package migration file %q: %v\n", m, err)
	}
}
