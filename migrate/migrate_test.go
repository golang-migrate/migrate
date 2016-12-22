package migrate

import (
	"io/ioutil"
	"os"
	"regexp"
	"testing"

	// Ensure imports for each driver we wish to test

	_ "github.com/mattes/migrate/driver/postgres"
	_ "github.com/mattes/migrate/driver/sqlite3"
)

// Add Driver URLs here to test basic Up, Down, .. functions.
var driverUrls = []string{
	"postgres://postgres@" + os.Getenv("POSTGRES_PORT_5432_TCP_ADDR") + ":" + os.Getenv("POSTGRES_PORT_5432_TCP_PORT") + "/template1?sslmode=disable",
}

func tearDown(driverUrl, tmpdir string) {
	DownSync(driverUrl, tmpdir)
	os.RemoveAll(tmpdir)
}

func TestCreate(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	for _, driverUrl := range driverUrls {
		t.Logf("Test driver: %s", driverUrl)
		tmpdir, err := ioutil.TempDir("/tmp", "migrate-test")
		if err != nil {
			t.Fatal(err)
		}
		defer tearDown(driverUrl, tmpdir)

		if _, err := Create(driverUrl, tmpdir, "test_migration"); err != nil {
			t.Fatal(err)
		}
		if _, err := Create(driverUrl, tmpdir, "another migration"); err != nil {
			t.Fatal(err)
		}

		files, err := ioutil.ReadDir(tmpdir)
		if err != nil {
			t.Fatal(err)
		}
		if len(files) != 4 {
			t.Fatal("Expected 4 new files, got", len(files))
		}
		fileNameRegexp := regexp.MustCompile(`^\d{10}_(.*.[up|down].sql)`)

		expectFiles := []string{
			"test_migration.up.sql", "test_migration.down.sql",
			"another_migration.up.sql", "another_migration.down.sql",
		}

		var foundCounter int

		for _, file := range files {
			if x := fileNameRegexp.FindStringSubmatch(file.Name()); len(x) != 2 {
				t.Errorf("expected %v to match %v", file.Name(), fileNameRegexp)
			} else {
				for _, expect := range expectFiles {
					if expect == x[1] {
						foundCounter++
						break
					}
				}

			}
		}

		if foundCounter != len(expectFiles) {
			t.Errorf("expected %v files, got %v", len(expectFiles), foundCounter)
		}
	}
}

func TestReset(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	for _, driverUrl := range driverUrls {
		t.Logf("Test driver: %s", driverUrl)
		tmpdir, err := ioutil.TempDir("/tmp", "migrate-test")
		if err != nil {
			t.Fatal(err)
		}
		defer tearDown(driverUrl, tmpdir)

		Create(driverUrl, tmpdir, "migration1")
		f, err := Create(driverUrl, tmpdir, "migration2")
		if err != nil {
			t.Fatal(err)
		}

		if err, ok := ResetSync(driverUrl, tmpdir); !ok {
			t.Fatal(err)
		}

		if version, err := Version(driverUrl, tmpdir); err != nil {
			t.Fatal(err)
		} else if version != f.Version {
			t.Fatalf("Expected version %v, got %v", version, f.Version)
		}
	}
}

func TestDown(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	for _, driverUrl := range driverUrls {
		t.Logf("Test driver: %s", driverUrl)
		tmpdir, err := ioutil.TempDir("/tmp", "migrate-test")
		if err != nil {
			t.Fatal(err)
		}
		defer tearDown(driverUrl, tmpdir)

		initVersion, _ := Version(driverUrl, tmpdir)

		firstMigration, _ := Create(driverUrl, tmpdir, "migration1")
		secondMigration, _ := Create(driverUrl, tmpdir, "migration2")

		t.Logf("init %v first %v second %v", initVersion, firstMigration.Version, secondMigration.Version)

		if err, ok := ResetSync(driverUrl, tmpdir); !ok {
			t.Fatal(err)
		}

		if version, err := Version(driverUrl, tmpdir); err != nil {
			t.Fatal(err)
		} else if version != secondMigration.Version {
			t.Fatalf("Expected version %v, got %v", version, secondMigration.Version)
		}

		if err, ok := DownSync(driverUrl, tmpdir); !ok {
			t.Fatal(err)
		}

		if version, err := Version(driverUrl, tmpdir); err != nil {
			t.Fatal(err)
		} else if version != 0 {
			t.Fatalf("Expected 0, got %v", version)
		}
	}
}

func TestUp(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	for _, driverUrl := range driverUrls {
		t.Logf("Test driver: %s", driverUrl)
		tmpdir, err := ioutil.TempDir("/tmp", "migrate-test")
		if err != nil {
			t.Fatal(err)
		}
		defer tearDown(driverUrl, tmpdir)

		initVersion, _ := Version(driverUrl, tmpdir)

		firstMigration, _ := Create(driverUrl, tmpdir, "migration1")
		secondMigration, _ := Create(driverUrl, tmpdir, "migration2")

		t.Logf("init %v first %v second %v", initVersion, firstMigration.Version, secondMigration.Version)

		if err, ok := DownSync(driverUrl, tmpdir); !ok {
			t.Fatal(err)
		}

		if version, err := Version(driverUrl, tmpdir); err != nil {
			t.Fatal(err)
		} else if version != initVersion {
			t.Fatalf("Expected initial version %v, got %v", initVersion, version)
		}

		if err, ok := UpSync(driverUrl, tmpdir); !ok {
			t.Fatal(err)
		}

		if version, err := Version(driverUrl, tmpdir); err != nil {
			t.Fatal(err)
		} else if version != secondMigration.Version {
			t.Fatalf("Expected migrated version %v, got %v", secondMigration.Version, version)
		}
	}
}

func TestRedo(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	for _, driverUrl := range driverUrls {
		t.Logf("Test driver: %s", driverUrl)
		tmpdir, err := ioutil.TempDir("/tmp", "migrate-test")
		if err != nil {
			t.Fatal(err)
		}
		defer tearDown(driverUrl, tmpdir)

		initVersion, _ := Version(driverUrl, tmpdir)

		firstMigration, _ := Create(driverUrl, tmpdir, "migration1")
		secondMigration, _ := Create(driverUrl, tmpdir, "migration2")

		t.Logf("init %v first %v second %v", initVersion, firstMigration.Version, secondMigration.Version)

		if err, ok := ResetSync(driverUrl, tmpdir); !ok {
			t.Fatal(err)
		}

		if version, err := Version(driverUrl, tmpdir); err != nil {
			t.Fatal(err)
		} else if version != secondMigration.Version {
			t.Fatalf("Expected migrated version %v, got %v", secondMigration.Version, version)
		}

		if err, ok := RedoSync(driverUrl, tmpdir); !ok {
			t.Fatal(err)
		}

		if version, err := Version(driverUrl, tmpdir); err != nil {
			t.Fatal(err)
		} else if version != secondMigration.Version {
			t.Fatalf("Expected migrated version %v, got %v", secondMigration.Version, version)
		}
	}
}

func TestMigrate(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	for _, driverUrl := range driverUrls {
		t.Logf("Test driver: %s", driverUrl)
		tmpdir, err := ioutil.TempDir("/tmp", "migrate-test")
		if err != nil {
			t.Fatal(err)
		}
		defer tearDown(driverUrl, tmpdir)

		initVersion, _ := Version(driverUrl, tmpdir)

		firstMigration, _ := Create(driverUrl, tmpdir, "migration1")
		secondMigration, _ := Create(driverUrl, tmpdir, "migration2")

		t.Logf("init %v first %v second %v", initVersion, firstMigration.Version, secondMigration.Version)

		if err, ok := ResetSync(driverUrl, tmpdir); !ok {
			t.Fatal(err)
		}

		if version, err := Version(driverUrl, tmpdir); err != nil {
			t.Fatal(err)
		} else if version != secondMigration.Version {
			t.Fatalf("Expected migrated version %v, got %v", secondMigration.Version, version)
		}

		if err, ok := MigrateSync(driverUrl, tmpdir, -2); !ok {
			t.Fatal(err)
		}

		if version, err := Version(driverUrl, tmpdir); err != nil {
			t.Fatal(err)
		} else if version != 0 {
			t.Fatalf("Expected 0, got %v", version)
		}

		if err, ok := MigrateSync(driverUrl, tmpdir, +1); !ok {
			t.Fatal(err)
		}

		if version, err := Version(driverUrl, tmpdir); err != nil {
			t.Fatal(err)
		} else if version != firstMigration.Version {
			t.Fatalf("Expected first version %v, got %v", firstMigration.Version, version)
		}
	}
}
