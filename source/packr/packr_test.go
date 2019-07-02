package packr

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	packr "github.com/gobuffalo/packr/v2"
	"github.com/golang-migrate/migrate/v4/source"
)

func TestPackr(t *testing.T) {
	testBox := packr.New("migrations", "|")
	boxMustAdd(t, testBox, "0001_first.up.sql", []byte("1 up"))
	boxMustAdd(t, testBox, "0001_first.down.sql", []byte("1 down"))
	boxMustAdd(t, testBox, "0002_second.up.sql", []byte("2 up"))
	boxMustAdd(t, testBox, "0002_second.down.sql", []byte("2 down"))
	boxMustAdd(t, testBox, "0003_third.up.sql", []byte("3 up"))
	boxMustAdd(t, testBox, "0003_third.down.sql", []byte("3 down"))

	ps, err := WithInstance(testBox)
	if err != nil {
		t.Fatal(err)
	}
	if ps == nil {
		t.Fatal("Packr Source was nil")
	}

	v, err := ps.First()
	if err != nil {
		t.Fatal(err)
	}
	testMigration(t, ps, v, "first")

	v, err = ps.Next(v)
	if err != nil {
		t.Fatal(err)
	}

	testMigration(t, ps, v, "second")

	v, err = ps.Next(v)
	if err != nil {
		t.Fatal(err)
	}

	testMigration(t, ps, v, "third")

	v, err = ps.Prev(v)
	if err != nil {
		t.Fatal(err)
	}

	testMigration(t, ps, v, "second")

	_, err = ps.Next(v)
	if err != nil {
		t.Fatal()
	}
	_, err = ps.Next(v)
	if err == nil {
		t.Fatal("Expected error")
	}
	if _, ok := err.(*os.PathError); !ok {
		t.Fatal("Expected the error to be a path error")
	}
}

func boxMustAdd(t *testing.T, box *packr.Box, name string, content []byte) {
	err := box.AddBytes(name, content)
	if err != nil {
		t.Fatalf("Failed to add testdata to box: %s", err)
	}
}

func testMigration(t *testing.T, ps source.Driver, v uint, expectedIdentifier string) {
	r, i, err := ps.ReadUp(v)
	if err != nil {
		t.Fatal(err)
	}
	if r == nil {
		t.Fatal("Migration ReadCloser was nil")
	}

	if i != expectedIdentifier {
		t.Fatalf("Expected different migration identifier: expected %s, got %s", expectedIdentifier, i)
	}

	migrationData, err := ioutil.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}

	expectedData := fmt.Sprintf("%d up", v)
	if string(migrationData) != expectedData {
		t.Fatalf("Failed to read correct migration data, expected %s, got %s", expectedData, string(migrationData))
	}

	r, i, err = ps.ReadDown(v)
	if err != nil {
		t.Fatal(err)
	}
	if r == nil {
		t.Fatal("Migration ReadCloser was nil")
	}

	if i != expectedIdentifier {
		t.Fatalf("Expected different migration identifier: expected %s, got %s", expectedIdentifier, i)
	}

	migrationData, err = ioutil.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}

	expectedData = fmt.Sprintf("%d down", v)
	if string(migrationData) != expectedData {
		t.Fatalf("Failed to read correct migration data, expected %s, got %s", expectedData, string(migrationData))
	}
}
