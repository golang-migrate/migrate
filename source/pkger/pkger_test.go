package pkger

import (
	"errors"
	"os"
	"testing"

	"github.com/gobuffalo/here"
	st "github.com/golang-migrate/migrate/v4/source/testing"
	"github.com/markbates/pkger"
	"github.com/markbates/pkger/pkging"
	"github.com/markbates/pkger/pkging/mem"
)

func Test(t *testing.T) {
	t.Run("WithInstance", func(t *testing.T) {
		i := testInstance(t)

		createPkgerFile(t, i, "/1_foobar.up.sql")
		createPkgerFile(t, i, "/1_foobar.down.sql")
		createPkgerFile(t, i, "/3_foobar.up.sql")
		createPkgerFile(t, i, "/4_foobar.up.sql")
		createPkgerFile(t, i, "/4_foobar.down.sql")
		createPkgerFile(t, i, "/5_foobar.down.sql")
		createPkgerFile(t, i, "/7_foobar.up.sql")
		createPkgerFile(t, i, "/7_foobar.down.sql")

		d, err := WithInstance(i, "/")
		if err != nil {
			t.Fatal(err)
		}

		st.Test(t, d)
	})

	t.Run("Open", func(t *testing.T) {
		i := testInstance(t)

		createPkgerFile(t, i, "/1_foobar.up.sql")
		createPkgerFile(t, i, "/1_foobar.down.sql")
		createPkgerFile(t, i, "/3_foobar.up.sql")
		createPkgerFile(t, i, "/4_foobar.up.sql")
		createPkgerFile(t, i, "/4_foobar.down.sql")
		createPkgerFile(t, i, "/5_foobar.down.sql")
		createPkgerFile(t, i, "/7_foobar.up.sql")
		createPkgerFile(t, i, "/7_foobar.down.sql")

		registerPackageLevelInstance(t, i)

		d, err := (&Pkger{}).Open("pkger:///")
		if err != nil {
			t.Fatal(err)
		}

		st.Test(t, d)
	})

}

func TestWithInstance(t *testing.T) {
	t.Run("Subdir", func(t *testing.T) {
		i := testInstance(t)

		// Make sure the relative root exists so that httpfs.PartialDriver can
		// initialize.
		createPkgerSubdir(t, i, "/subdir")

		_, err := WithInstance(i, "/subdir")
		if err != nil {
			t.Fatal("")
		}
	})

	t.Run("NilInstance", func(t *testing.T) {
		_, err := WithInstance(nil, "")
		if err == nil {
			t.Fatal(err)
		}
	})

	t.Run("FailInit", func(t *testing.T) {
		i := testInstance(t)

		_, err := WithInstance(i, "/fail")
		if err == nil {
			t.Fatal(err)
		}
	})

	t.Run("FailWithoutMigrations", func(t *testing.T) {
		i := testInstance(t)

		createPkgerSubdir(t, i, "/")

		d, err := WithInstance(i, "/")
		if err != nil {
			t.Fatal(err)
		}

		if _, err := d.First(); !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}

	})
}

func TestOpen(t *testing.T) {

	t.Run("InvalidURL", func(t *testing.T) {
		_, err := (&Pkger{}).Open(":///")
		if err == nil {
			t.Fatal(err)
		}
	})

	t.Run("Root", func(t *testing.T) {
		_, err := (&Pkger{}).Open("pkger:///")
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("FailInit", func(t *testing.T) {
		_, err := (&Pkger{}).Open("pkger:///subdir")
		if err == nil {
			t.Fatal(err)
		}
	})

	i := testInstance(t)
	createPkgerSubdir(t, i, "/subdir")

	// Note that this registers the instance globally so anything run after
	// this will have access to everything container in the registered
	// instance.
	registerPackageLevelInstance(t, i)

	t.Run("Subdir", func(t *testing.T) {
		_, err := (&Pkger{}).Open("pkger:///subdir")
		if err != nil {
			t.Fatal(err)
		}
	})
}

func TestClose(t *testing.T) {
	d, err := (&Pkger{}).Open("pkger:///")
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
}

func registerPackageLevelInstance(t *testing.T, pkg pkging.Pkger) {
	if err := pkger.Apply(pkg, nil); err != nil {
		t.Fatalf("failed to register pkger instance: %v\n", err)
	}
}

func testInstance(t *testing.T) pkging.Pkger {
	pkg, err := inMemoryPkger()
	if err != nil {
		t.Fatalf("failed to create an  pkging.Pkger instance: %v\n", err)
	}

	return pkg
}

func createPkgerSubdir(t *testing.T, pkg pkging.Pkger, subdir string) {
	if err := pkg.MkdirAll(subdir, os.ModePerm); err != nil {
		t.Fatalf("failed to create pkger subdir %q: %v\n", subdir, err)
	}
}

func createPkgerFile(t *testing.T, pkg pkging.Pkger, name string) {
	_, err := pkg.Create(name)
	if err != nil {
		t.Fatalf("failed to create pkger file %q: %v\n", name, err)
	}
}

func inMemoryPkger() (*mem.Pkger, error) {
	info, err := here.New().Current()
	if err != nil {
		return nil, err
	}

	pkg, err := mem.New(info)
	if err != nil {
		return nil, err
	}

	return pkg, nil
}
