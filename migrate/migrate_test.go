package migrate

import (
	"io/ioutil"
	"testing"
)

func TestCreate(t *testing.T) {
	tmpdir, err := ioutil.TempDir("/tmp", "migrate-postgres-test")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Create("postgres://localhost/migratetest?sslmode=disable", tmpdir, "test_migration"); err != nil {
		t.Fatal(err)
	}
	if _, err := Create("postgres://localhost/migratetest?sslmode=disable", tmpdir, "another migration"); err != nil {
		t.Fatal(err)
	}

	files, err := ioutil.ReadDir(tmpdir)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 4 {
		t.Fatal("Expected 2 new files, got", len(files))
	}
	expectFiles := []string{
		"0001_test_migration.up.sql", "0001_test_migration.down.sql",
		"0002_another_migration.up.sql", "0002_another_migration.down.sql",
	}
	foundCounter := 0
	for _, expectFile := range expectFiles {
		for _, file := range files {
			if expectFile == file.Name() {
				foundCounter += 1
				break
			}
		}
	}
	if foundCounter != len(expectFiles) {
		t.Error("not all expected files have been found")
	}
}
