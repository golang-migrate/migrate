package migrate

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"io"
	"log"
	"os"
	"strings"
	"testing"

	dStub "github.com/golang-migrate/migrate/v4/database/stub"
	"github.com/golang-migrate/migrate/v4/source"
	sStub "github.com/golang-migrate/migrate/v4/source/stub"
)

// sourceStubMigrations hold the following migrations:
// u = up migration, d = down migration, n = version
//
//	|  1  |  -  |  3  |  4  |  5  |  -  |  7  |
//	| u d |  -  | u   | u d |   d |  -  | u d |
var sourceStubMigrations *source.Migrations

const (
	srcDrvNameStub = "stub"
	dbDrvNameStub  = "stub"
)

func init() {
	sourceStubMigrations = source.NewMigrations()
	sourceStubMigrations.Append(&source.Migration{Version: 1, Direction: source.Up, Identifier: "CREATE 1"})
	sourceStubMigrations.Append(&source.Migration{Version: 1, Direction: source.Down, Identifier: "DROP 1"})
	sourceStubMigrations.Append(&source.Migration{Version: 3, Direction: source.Up, Identifier: "CREATE 3"})
	sourceStubMigrations.Append(&source.Migration{Version: 4, Direction: source.Up, Identifier: "CREATE 4"})
	sourceStubMigrations.Append(&source.Migration{Version: 4, Direction: source.Down, Identifier: "DROP 4"})
	sourceStubMigrations.Append(&source.Migration{Version: 5, Direction: source.Down, Identifier: "DROP 5"})
	sourceStubMigrations.Append(&source.Migration{Version: 7, Direction: source.Up, Identifier: "CREATE 7"})
	sourceStubMigrations.Append(&source.Migration{Version: 7, Direction: source.Down, Identifier: "DROP 7"})
}

type DummyInstance struct{ Name string }

func TestNew(t *testing.T) {
	ctx := context.Background()
	m, err := New(ctx, "stub://", "stub://")
	if err != nil {
		t.Fatal(err)
	}

	if m.sourceName != srcDrvNameStub {
		t.Errorf("expected stub, got %v", m.sourceName)
	}
	if m.sourceDrv == nil {
		t.Error("expected sourceDrv not to be nil")
	}

	if m.databaseName != dbDrvNameStub {
		t.Errorf("expected stub, got %v", m.databaseName)
	}
	if m.databaseDrv == nil {
		t.Error("expected databaseDrv not to be nil")
	}
}

func ExampleNew() {
	// Read migrations from /home/mattes/migrations and connect to a local postgres database.
	ctx := context.Background()
	m, err := New(ctx, "file:///home/mattes/migrations", "postgres://mattes:secret@localhost:5432/database?sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}

	// Migrate all the way up ...
	if err := m.Up(ctx); err != nil && err != ErrNoChange {
		log.Fatal(err)
	}
}

func TestNewWithDatabaseInstance(t *testing.T) {
	ctx := context.Background()
	dummyDb := &DummyInstance{"database"}
	dbInst, err := dStub.WithInstance(ctx, dummyDb, &dStub.Config{})
	if err != nil {
		t.Fatal(err)
	}

	m, err := NewWithDatabaseInstance(ctx, "stub://", dbDrvNameStub, dbInst)
	if err != nil {
		t.Fatal(err)
	}

	if m.sourceName != srcDrvNameStub {
		t.Errorf("expected stub, got %v", m.sourceName)
	}
	if m.sourceDrv == nil {
		t.Error("expected sourceDrv not to be nil")
	}

	if m.databaseName != dbDrvNameStub {
		t.Errorf("expected stub, got %v", m.databaseName)
	}
	if m.databaseDrv == nil {
		t.Error("expected databaseDrv not to be nil")
	}
}

func ExampleNewWithDatabaseInstance() {
	ctx := context.Background()

	// Create and use an existing database instance.
	db, err := sql.Open("postgres", "postgres://mattes:secret@localhost:5432/database?sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	// Create driver instance from db.
	// Check each driver if it supports the WithInstance function.
	// `import "github.com/golang-migrate/migrate/v4/database/postgres"`
	instance, err := dStub.WithInstance(ctx, db, &dStub.Config{})
	if err != nil {
		log.Fatal(err)
	}

	// Read migrations from /home/mattes/migrations and connect to a local postgres database.
	m, err := NewWithDatabaseInstance(ctx, "file:///home/mattes/migrations", "postgres", instance)
	if err != nil {
		log.Fatal(err)
	}

	// Migrate all the way up ...
	if err := m.Up(ctx); err != nil {
		log.Fatal(err)
	}
}

func TestNewWithSourceInstance(t *testing.T) {
	ctx := context.Background()
	dummySource := &DummyInstance{"source"}
	sInst, err := sStub.WithInstance(ctx, dummySource, &sStub.Config{})
	if err != nil {
		t.Fatal(err)
	}

	m, err := NewWithSourceInstance(ctx, srcDrvNameStub, sInst, "stub://")
	if err != nil {
		t.Fatal(err)
	}

	if m.sourceName != srcDrvNameStub {
		t.Errorf("expected stub, got %v", m.sourceName)
	}
	if m.sourceDrv == nil {
		t.Error("expected sourceDrv not to be nil")
	}

	if m.databaseName != dbDrvNameStub {
		t.Errorf("expected stub, got %v", m.databaseName)
	}
	if m.databaseDrv == nil {
		t.Error("expected databaseDrv not to be nil")
	}
}

func ExampleNewWithSourceInstance() {
	ctx := context.Background()
	di := &DummyInstance{"think any client required for a source here"}

	// Create driver instance from DummyInstance di.
	// Check each driver if it support the WithInstance function.
	// `import "github.com/golang-migrate/migrate/v4/source/stub"`
	instance, err := sStub.WithInstance(ctx, di, &sStub.Config{})
	if err != nil {
		log.Fatal(err)
	}

	// Read migrations from Stub and connect to a local postgres database.
	m, err := NewWithSourceInstance(ctx, srcDrvNameStub, instance, "postgres://mattes:secret@localhost:5432/database?sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}

	// Migrate all the way up ...
	if err := m.Up(ctx); err != nil {
		log.Fatal(err)
	}
}

func TestNewWithInstance(t *testing.T) {
	ctx := context.Background()
	dummyDb := &DummyInstance{"database"}
	dbInst, err := dStub.WithInstance(ctx, dummyDb, &dStub.Config{})
	if err != nil {
		t.Fatal(err)
	}

	dummySource := &DummyInstance{"source"}
	sInst, err := sStub.WithInstance(ctx, dummySource, &sStub.Config{})
	if err != nil {
		t.Fatal(err)
	}

	m, err := NewWithInstance(srcDrvNameStub, sInst, dbDrvNameStub, dbInst)
	if err != nil {
		t.Fatal(err)
	}

	if m.sourceName != srcDrvNameStub {
		t.Errorf("expected stub, got %v", m.sourceName)
	}
	if m.sourceDrv == nil {
		t.Error("expected sourceDrv not to be nil")
	}

	if m.databaseName != dbDrvNameStub {
		t.Errorf("expected stub, got %v", m.databaseName)
	}
	if m.databaseDrv == nil {
		t.Error("expected databaseDrv not to be nil")
	}
}

func ExampleNewWithInstance() {
	// See NewWithDatabaseInstance and NewWithSourceInstance for an example.
}

func TestClose(t *testing.T) {
	ctx := context.Background()
	m, _ := New(ctx, "stub://", "stub://")
	sourceErr, databaseErr := m.Close(ctx)
	if sourceErr != nil {
		t.Error(sourceErr)
	}
	if databaseErr != nil {
		t.Error(databaseErr)
	}
}

func TestMigrate(t *testing.T) {
	ctx := context.Background()
	m, _ := New(ctx, "stub://", "stub://")
	m.sourceDrv.(*sStub.Stub).Migrations = sourceStubMigrations
	dbDrv := m.databaseDrv.(*dStub.Stub)

	tt := []struct {
		version       uint
		expectErr     error
		expectVersion uint
		expectSeq     migrationSequence
	}{
		// migrate all the way Up in single steps
		{
			version:   0,
			expectErr: os.ErrNotExist,
		},
		{
			version:       1,
			expectVersion: 1,
			expectSeq: migrationSequence{
				mr("CREATE 1"),
			},
		},
		{
			version:   2,
			expectErr: os.ErrNotExist,
			expectSeq: migrationSequence{
				mr("CREATE 1"),
			},
		},
		{
			version:       3,
			expectVersion: 3,
			expectSeq: migrationSequence{
				mr("CREATE 1"),
				mr("CREATE 3"),
			},
		},
		{
			version:       4,
			expectVersion: 4,
			expectSeq: migrationSequence{
				mr("CREATE 1"),
				mr("CREATE 3"),
				mr("CREATE 4"),
			},
		},
		{
			version:       5,
			expectVersion: 5,
			expectSeq: migrationSequence{ // 5 has no up migration
				mr("CREATE 1"),
				mr("CREATE 3"),
				mr("CREATE 4"),
			},
		},
		{
			version:   6,
			expectErr: os.ErrNotExist,
			expectSeq: migrationSequence{
				mr("CREATE 1"),
				mr("CREATE 3"),
				mr("CREATE 4"),
			},
		},
		{
			version:       7,
			expectVersion: 7,
			expectSeq: migrationSequence{
				mr("CREATE 1"),
				mr("CREATE 3"),
				mr("CREATE 4"),
				mr("CREATE 7"),
			},
		},
		{
			version:   8,
			expectErr: os.ErrNotExist,
			expectSeq: migrationSequence{
				mr("CREATE 1"),
				mr("CREATE 3"),
				mr("CREATE 4"),
				mr("CREATE 7"),
			},
		},

		// migrate all the way Down in single steps
		{
			version:   6,
			expectErr: os.ErrNotExist,
			expectSeq: migrationSequence{
				mr("CREATE 1"),
				mr("CREATE 3"),
				mr("CREATE 4"),
				mr("CREATE 7"),
			},
		},
		{
			version:       5,
			expectVersion: 5,
			expectSeq: migrationSequence{
				mr("CREATE 1"),
				mr("CREATE 3"),
				mr("CREATE 4"),
				mr("CREATE 7"),
				mr("DROP 7"),
			},
		},
		{
			version:       4,
			expectVersion: 4,
			expectSeq: migrationSequence{
				mr("CREATE 1"),
				mr("CREATE 3"),
				mr("CREATE 4"),
				mr("CREATE 7"),
				mr("DROP 7"),
				mr("DROP 5"),
			},
		},
		{
			version:       3,
			expectVersion: 3,
			expectSeq: migrationSequence{
				mr("CREATE 1"),
				mr("CREATE 3"),
				mr("CREATE 4"),
				mr("CREATE 7"),
				mr("DROP 7"),
				mr("DROP 5"),
				mr("DROP 4"),
			},
		},
		{
			version:   2,
			expectErr: os.ErrNotExist,
			expectSeq: migrationSequence{
				mr("CREATE 1"),
				mr("CREATE 3"),
				mr("CREATE 4"),
				mr("CREATE 7"),
				mr("DROP 7"),
				mr("DROP 5"),
				mr("DROP 4"),
			},
		},
		{
			version:       1,
			expectVersion: 1,
			expectSeq: migrationSequence{ // 3 has no down migration
				mr("CREATE 1"),
				mr("CREATE 3"),
				mr("CREATE 4"),
				mr("CREATE 7"),
				mr("DROP 7"),
				mr("DROP 5"),
				mr("DROP 4"),
			},
		},
		{
			version:   0,
			expectErr: os.ErrNotExist,
			expectSeq: migrationSequence{
				mr("CREATE 1"),
				mr("CREATE 3"),
				mr("CREATE 4"),
				mr("CREATE 7"),
				mr("DROP 7"),
				mr("DROP 5"),
				mr("DROP 4"),
			},
		},

		// migrate all the way Up in one step
		{
			version:       7,
			expectVersion: 7,
			expectSeq: migrationSequence{
				mr("CREATE 1"),
				mr("CREATE 3"),
				mr("CREATE 4"),
				mr("CREATE 7"),
				mr("DROP 7"),
				mr("DROP 5"),
				mr("DROP 4"),
				mr("CREATE 3"),
				mr("CREATE 4"),
				mr("CREATE 7"),
			},
		},

		// migrate all the way Down in one step
		{
			version:       1,
			expectVersion: 1,
			expectSeq: migrationSequence{
				mr("CREATE 1"),
				mr("CREATE 3"),
				mr("CREATE 4"),
				mr("CREATE 7"),
				mr("DROP 7"),
				mr("DROP 5"),
				mr("DROP 4"),
				mr("CREATE 3"),
				mr("CREATE 4"),
				mr("CREATE 7"),
				mr("DROP 7"),
				mr("DROP 5"),
				mr("DROP 4"),
			},
		},

		// can't migrate the same version twice
		{
			version:   1,
			expectErr: ErrNoChange,
			expectSeq: migrationSequence{
				mr("CREATE 1"),
				mr("CREATE 3"),
				mr("CREATE 4"),
				mr("CREATE 7"),
				mr("DROP 7"),
				mr("DROP 5"),
				mr("DROP 4"),
				mr("CREATE 3"),
				mr("CREATE 4"),
				mr("CREATE 7"),
				mr("DROP 7"),
				mr("DROP 5"),
				mr("DROP 4"),
			},
		},
	}

	for i, v := range tt {
		err := m.Migrate(ctx, v.version)
		if (v.expectErr == os.ErrNotExist && !errors.Is(err, os.ErrNotExist)) ||
			(v.expectErr != os.ErrNotExist && err != v.expectErr) {
			t.Errorf("expected err %v, got %v, in %v", v.expectErr, err, i)

		} else if err == nil {
			version, _, err := m.Version(ctx)
			if err != nil {
				t.Error(err)
			}
			if version != v.expectVersion {
				t.Errorf("expected version %v, got %v, in %v", v.expectVersion, version, i)
			}
		}
		equalDbSeq(t, i, v.expectSeq, dbDrv)
	}
}

func TestMigrateDirty(t *testing.T) {
	ctx := context.Background()
	m, _ := New(ctx, "stub://", "stub://")
	dbDrv := m.databaseDrv.(*dStub.Stub)
	if err := dbDrv.SetVersion(ctx, 0, true); err != nil {
		t.Fatal(err)
	}

	err := m.Migrate(ctx, 1)
	if _, ok := err.(ErrDirty); !ok {
		t.Fatalf("expected ErrDirty, got %v", err)
	}
}

func TestSteps(t *testing.T) {
	ctx := context.Background()
	m, _ := New(ctx, "stub://", "stub://")
	m.sourceDrv.(*sStub.Stub).Migrations = sourceStubMigrations
	dbDrv := m.databaseDrv.(*dStub.Stub)

	tt := []struct {
		steps         int
		expectErr     error
		expectVersion int
		expectSeq     migrationSequence
	}{
		// step must be != 0
		{
			steps:     0,
			expectErr: ErrNoChange,
		},

		// can't go Down if ErrNilVersion
		{
			steps:     -1,
			expectErr: os.ErrNotExist,
		},

		// migrate all the way Up
		{
			steps:         1,
			expectVersion: 1,
			expectSeq: migrationSequence{
				mr("CREATE 1")},
		},
		{
			steps:         1,
			expectVersion: 3,
			expectSeq: migrationSequence{
				mr("CREATE 1"),
				mr("CREATE 3"),
			},
		},
		{
			steps:         1,
			expectVersion: 4,
			expectSeq: migrationSequence{
				mr("CREATE 1"),
				mr("CREATE 3"),
				mr("CREATE 4"),
			},
		},
		{
			steps:         1,
			expectVersion: 5,
			expectSeq: migrationSequence{
				mr("CREATE 1"),
				mr("CREATE 3"),
				mr("CREATE 4"),
			},
		},
		{
			steps:         1,
			expectVersion: 7,
			expectSeq: migrationSequence{
				mr("CREATE 1"),
				mr("CREATE 3"),
				mr("CREATE 4"),
				mr("CREATE 7"),
			},
		},
		{
			steps:     1,
			expectErr: os.ErrNotExist,
			expectSeq: migrationSequence{
				mr("CREATE 1"),
				mr("CREATE 3"),
				mr("CREATE 4"),
				mr("CREATE 7"),
			},
		},

		// migrate all the way Down
		{
			steps:         -1,
			expectVersion: 5,
			expectSeq: migrationSequence{
				mr("CREATE 1"),
				mr("CREATE 3"),
				mr("CREATE 4"),
				mr("CREATE 7"),
				mr("DROP 7"),
			},
		},
		{
			steps:         -1,
			expectVersion: 4,
			expectSeq: migrationSequence{
				mr("CREATE 1"),
				mr("CREATE 3"),
				mr("CREATE 4"),
				mr("CREATE 7"),
				mr("DROP 7"),
				mr("DROP 5"),
			},
		},
		{
			steps:         -1,
			expectVersion: 3,
			expectSeq: migrationSequence{
				mr("CREATE 1"),
				mr("CREATE 3"),
				mr("CREATE 4"),
				mr("CREATE 7"),
				mr("DROP 7"),
				mr("DROP 5"),
				mr("DROP 4"),
			},
		},
		{
			steps:         -1,
			expectVersion: 1,
			expectSeq: migrationSequence{
				mr("CREATE 1"),
				mr("CREATE 3"),
				mr("CREATE 4"),
				mr("CREATE 7"),
				mr("DROP 7"),
				mr("DROP 5"),
				mr("DROP 4"),
			},
		},
		{
			steps:         -1,
			expectVersion: -1,
			expectSeq: migrationSequence{
				mr("CREATE 1"),
				mr("CREATE 3"),
				mr("CREATE 4"),
				mr("CREATE 7"),
				mr("DROP 7"),
				mr("DROP 5"),
				mr("DROP 4"),
				mr("DROP 1"),
			},
		},

		// migrate Up in bigger step
		{
			steps:         4,
			expectVersion: 5,
			expectSeq: migrationSequence{
				mr("CREATE 1"),
				mr("CREATE 3"),
				mr("CREATE 4"),
				mr("CREATE 7"),
				mr("DROP 7"),
				mr("DROP 5"),
				mr("DROP 4"),
				mr("DROP 1"),
				mr("CREATE 1"),
				mr("CREATE 3"),
				mr("CREATE 4"),
			},
		},

		// apply one migration, then reaches out of boundary
		{
			steps:         2,
			expectErr:     ErrShortLimit{1},
			expectVersion: 7,
			expectSeq: migrationSequence{
				mr("CREATE 1"),
				mr("CREATE 3"),
				mr("CREATE 4"),
				mr("CREATE 7"),
				mr("DROP 7"),
				mr("DROP 5"),
				mr("DROP 4"),
				mr("DROP 1"),
				mr("CREATE 1"),
				mr("CREATE 3"),
				mr("CREATE 4"),
				mr("CREATE 7"),
			},
		},

		// migrate Down in bigger step
		{
			steps:         -4,
			expectVersion: 1,
			expectSeq: migrationSequence{
				mr("CREATE 1"),
				mr("CREATE 3"),
				mr("CREATE 4"),
				mr("CREATE 7"),
				mr("DROP 7"),
				mr("DROP 5"),
				mr("DROP 4"),
				mr("DROP 1"),
				mr("CREATE 1"),
				mr("CREATE 3"),
				mr("CREATE 4"),
				mr("CREATE 7"),
				mr("DROP 7"),
				mr("DROP 5"),
				mr("DROP 4"),
			},
		},

		// apply one migration, then reaches out of boundary
		{
			steps:         -2,
			expectErr:     ErrShortLimit{1},
			expectVersion: -1,
			expectSeq: migrationSequence{
				mr("CREATE 1"),
				mr("CREATE 3"),
				mr("CREATE 4"),
				mr("CREATE 7"),
				mr("DROP 7"),
				mr("DROP 5"),
				mr("DROP 4"),
				mr("DROP 1"),
				mr("CREATE 1"),
				mr("CREATE 3"),
				mr("CREATE 4"),
				mr("CREATE 7"),
				mr("DROP 7"),
				mr("DROP 5"),
				mr("DROP 4"),
				mr("DROP 1"),
			},
		},
	}

	for i, v := range tt {
		err := m.Steps(ctx, v.steps)
		if (v.expectErr == os.ErrNotExist && !errors.Is(err, os.ErrNotExist)) ||
			(v.expectErr != os.ErrNotExist && err != v.expectErr) {
			t.Errorf("expected err %v, got %v, in %v", v.expectErr, err, i)

		} else if err == nil {
			version, _, err := m.Version(ctx)
			if err != ErrNilVersion && err != nil {
				t.Error(err)
			}
			if v.expectVersion == -1 && err != ErrNilVersion {
				t.Errorf("expected ErrNilVersion, got %v, in %v", version, i)

			} else if v.expectVersion >= 0 && version != uint(v.expectVersion) {
				t.Errorf("expected version %v, got %v, in %v", v.expectVersion, version, i)
			}
		}
		equalDbSeq(t, i, v.expectSeq, dbDrv)
	}
}

func TestStepsDirty(t *testing.T) {
	ctx := context.Background()
	m, _ := New(ctx, "stub://", "stub://")
	dbDrv := m.databaseDrv.(*dStub.Stub)
	if err := dbDrv.SetVersion(ctx, 0, true); err != nil {
		t.Fatal(err)
	}

	err := m.Steps(ctx, 1)
	if _, ok := err.(ErrDirty); !ok {
		t.Fatalf("expected ErrDirty, got %v", err)
	}
}

func TestUpAndDown(t *testing.T) {
	ctx := context.Background()
	m, _ := New(ctx, "stub://", "stub://")
	m.sourceDrv.(*sStub.Stub).Migrations = sourceStubMigrations
	dbDrv := m.databaseDrv.(*dStub.Stub)

	// go Up first
	if err := m.Up(ctx); err != nil {
		t.Fatal(err)
	}
	expectedSequence := migrationSequence{
		mr("CREATE 1"),
		mr("CREATE 3"),
		mr("CREATE 4"),
		mr("CREATE 7"),
	}
	equalDbSeq(t, 0, expectedSequence, dbDrv)

	// go Down
	if err := m.Down(ctx); err != nil {
		t.Fatal(err)
	}
	expectedSequence = migrationSequence{
		mr("CREATE 1"),
		mr("CREATE 3"),
		mr("CREATE 4"),
		mr("CREATE 7"),
		mr("DROP 7"),
		mr("DROP 5"),
		mr("DROP 4"),
		mr("DROP 1"),
	}
	equalDbSeq(t, 1, expectedSequence, dbDrv)

	// go 1 Up and then all the way Up
	if err := m.Steps(ctx, 1); err != nil {
		t.Fatal(err)
	}
	expectedSequence = migrationSequence{
		mr("CREATE 1"),
		mr("CREATE 3"),
		mr("CREATE 4"),
		mr("CREATE 7"),
		mr("DROP 7"),
		mr("DROP 5"),
		mr("DROP 4"),
		mr("DROP 1"),
		mr("CREATE 1"),
	}
	equalDbSeq(t, 2, expectedSequence, dbDrv)

	if err := m.Up(ctx); err != nil {
		t.Fatal(err)
	}
	expectedSequence = migrationSequence{
		mr("CREATE 1"),
		mr("CREATE 3"),
		mr("CREATE 4"),
		mr("CREATE 7"),
		mr("DROP 7"),
		mr("DROP 5"),
		mr("DROP 4"),
		mr("DROP 1"),
		mr("CREATE 1"),
		mr("CREATE 3"),
		mr("CREATE 4"),
		mr("CREATE 7"),
	}
	equalDbSeq(t, 3, expectedSequence, dbDrv)

	// go 1 Down and then all the way Down
	if err := m.Steps(ctx, -1); err != nil {
		t.Fatal(err)
	}
	expectedSequence = migrationSequence{
		mr("CREATE 1"),
		mr("CREATE 3"),
		mr("CREATE 4"),
		mr("CREATE 7"),
		mr("DROP 7"),
		mr("DROP 5"),
		mr("DROP 4"),
		mr("DROP 1"),
		mr("CREATE 1"),
		mr("CREATE 3"),
		mr("CREATE 4"),
		mr("CREATE 7"),
		mr("DROP 7"),
	}
	equalDbSeq(t, 1, expectedSequence, dbDrv)

	if err := m.Down(ctx); err != nil {
		t.Fatal(err)
	}
	expectedSequence = migrationSequence{
		mr("CREATE 1"),
		mr("CREATE 3"),
		mr("CREATE 4"),
		mr("CREATE 7"),
		mr("DROP 7"),
		mr("DROP 5"),
		mr("DROP 4"),
		mr("DROP 1"),
		mr("CREATE 1"),
		mr("CREATE 3"),
		mr("CREATE 4"),
		mr("CREATE 7"),
		mr("DROP 7"),
		mr("DROP 5"),
		mr("DROP 4"),
		mr("DROP 1"),
	}
	equalDbSeq(t, 1, expectedSequence, dbDrv)
}

func TestUpDirty(t *testing.T) {
	ctx := context.Background()
	m, _ := New(ctx, "stub://", "stub://")
	dbDrv := m.databaseDrv.(*dStub.Stub)
	if err := dbDrv.SetVersion(ctx, 0, true); err != nil {
		t.Fatal(err)
	}

	err := m.Up(ctx)
	if _, ok := err.(ErrDirty); !ok {
		t.Fatalf("expected ErrDirty, got %v", err)
	}
}

func TestDownDirty(t *testing.T) {
	ctx := context.Background()
	m, _ := New(ctx, "stub://", "stub://")
	dbDrv := m.databaseDrv.(*dStub.Stub)
	if err := dbDrv.SetVersion(ctx, 0, true); err != nil {
		t.Fatal(err)
	}

	err := m.Down(ctx)
	if _, ok := err.(ErrDirty); !ok {
		t.Fatalf("expected ErrDirty, got %v", err)
	}
}

func TestDrop(t *testing.T) {
	ctx := context.Background()
	m, _ := New(ctx, "stub://", "stub://")
	m.sourceDrv.(*sStub.Stub).Migrations = sourceStubMigrations
	dbDrv := m.databaseDrv.(*dStub.Stub)

	if err := m.Drop(ctx); err != nil {
		t.Fatal(err)
	}

	if dbDrv.MigrationSequence[len(dbDrv.MigrationSequence)-1] != dStub.DROP {
		t.Fatalf("expected database to DROP, got sequence %v", dbDrv.MigrationSequence)
	}
}

func TestVersion(t *testing.T) {
	ctx := context.Background()
	m, _ := New(ctx, "stub://", "stub://")
	dbDrv := m.databaseDrv.(*dStub.Stub)

	_, _, err := m.Version(ctx)
	if err != ErrNilVersion {
		t.Fatalf("expected ErrNilVersion, got %v", err)
	}

	if err := dbDrv.Run(ctx, bytes.NewBufferString("1_up")); err != nil {
		t.Fatal(err)
	}

	if err := dbDrv.SetVersion(ctx, 1, false); err != nil {
		t.Fatal(err)
	}

	v, _, err := m.Version(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if v != 1 {
		t.Fatalf("expected version 1, got %v", v)
	}
}

func TestRun(t *testing.T) {
	ctx := context.Background()
	m, _ := New(ctx, "stub://", "stub://")

	mx, err := NewMigration(nil, "", 1, 2)
	if err != nil {
		t.Fatal(err)
	}

	if err := m.Run(ctx, mx); err != nil {
		t.Fatal(err)
	}

	v, _, err := m.Version(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if v != 2 {
		t.Errorf("expected version 2, got %v", v)
	}
}

func TestRunDirty(t *testing.T) {
	ctx := context.Background()
	m, _ := New(ctx, "stub://", "stub://")
	dbDrv := m.databaseDrv.(*dStub.Stub)
	if err := dbDrv.SetVersion(ctx, 0, true); err != nil {
		t.Fatal(err)
	}

	migr, err := NewMigration(nil, "", 1, 2)
	if err != nil {
		t.Fatal(err)
	}

	err = m.Run(ctx, migr)
	if _, ok := err.(ErrDirty); !ok {
		t.Fatalf("expected ErrDirty, got %v", err)
	}
}

func TestForce(t *testing.T) {
	ctx := context.Background()
	m, _ := New(ctx, "stub://", "stub://")
	m.sourceDrv.(*sStub.Stub).Migrations = sourceStubMigrations

	if err := m.Force(ctx, 7); err != nil {
		t.Fatal(err)
	}

	v, dirty, err := m.Version(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if dirty {
		t.Errorf("expected dirty to be false")
	}
	if v != 7 {
		t.Errorf("expected version to be 7")
	}
}

func TestForceDirty(t *testing.T) {
	ctx := context.Background()
	m, _ := New(ctx, "stub://", "stub://")
	dbDrv := m.databaseDrv.(*dStub.Stub)
	if err := dbDrv.SetVersion(ctx, 0, true); err != nil {
		t.Fatal(err)
	}

	if err := m.Force(ctx, 1); err != nil {
		t.Fatal(err)
	}
}

func TestRead(t *testing.T) {
	ctx := context.Background()
	m, _ := New(ctx, "stub://", "stub://")
	m.sourceDrv.(*sStub.Stub).Migrations = sourceStubMigrations

	tt := []struct {
		from             int
		to               int
		expectErr        error
		expectMigrations migrationSequence
	}{
		{from: -1, to: -1, expectErr: ErrNoChange},
		{from: -1, to: 0, expectErr: os.ErrNotExist},
		{from: -1, to: 1, expectErr: nil, expectMigrations: newMigSeq(M(ctx, 1))},
		{from: -1, to: 2, expectErr: os.ErrNotExist},
		{from: -1, to: 3, expectErr: nil, expectMigrations: newMigSeq(M(ctx, 1), M(ctx, 3))},
		{from: -1, to: 4, expectErr: nil, expectMigrations: newMigSeq(M(ctx, 1), M(ctx, 3), M(ctx, 4))},
		{from: -1, to: 5, expectErr: nil, expectMigrations: newMigSeq(M(ctx, 1), M(ctx, 3), M(ctx, 4), M(ctx, 5))},
		{from: -1, to: 6, expectErr: os.ErrNotExist},
		{from: -1, to: 7, expectErr: nil, expectMigrations: newMigSeq(M(ctx, 1), M(ctx, 3), M(ctx, 4), M(ctx, 5), M(ctx, 7))},
		{from: -1, to: 8, expectErr: os.ErrNotExist},

		{from: 0, to: -1, expectErr: os.ErrNotExist},
		{from: 0, to: 0, expectErr: os.ErrNotExist},
		{from: 0, to: 1, expectErr: os.ErrNotExist},
		{from: 0, to: 2, expectErr: os.ErrNotExist},
		{from: 0, to: 3, expectErr: os.ErrNotExist},
		{from: 0, to: 4, expectErr: os.ErrNotExist},
		{from: 0, to: 5, expectErr: os.ErrNotExist},
		{from: 0, to: 6, expectErr: os.ErrNotExist},
		{from: 0, to: 7, expectErr: os.ErrNotExist},
		{from: 0, to: 8, expectErr: os.ErrNotExist},

		{from: 1, to: -1, expectErr: nil, expectMigrations: newMigSeq(M(ctx, 1, -1))},
		{from: 1, to: 0, expectErr: os.ErrNotExist},
		{from: 1, to: 1, expectErr: ErrNoChange},
		{from: 1, to: 2, expectErr: os.ErrNotExist},
		{from: 1, to: 3, expectErr: nil, expectMigrations: newMigSeq(M(ctx, 3))},
		{from: 1, to: 4, expectErr: nil, expectMigrations: newMigSeq(M(ctx, 3), M(ctx, 4))},
		{from: 1, to: 5, expectErr: nil, expectMigrations: newMigSeq(M(ctx, 3), M(ctx, 4), M(ctx, 5))},
		{from: 1, to: 6, expectErr: os.ErrNotExist},
		{from: 1, to: 7, expectErr: nil, expectMigrations: newMigSeq(M(ctx, 3), M(ctx, 4), M(ctx, 5), M(ctx, 7))},
		{from: 1, to: 8, expectErr: os.ErrNotExist},

		{from: 2, to: -1, expectErr: os.ErrNotExist},
		{from: 2, to: 0, expectErr: os.ErrNotExist},
		{from: 2, to: 1, expectErr: os.ErrNotExist},
		{from: 2, to: 2, expectErr: os.ErrNotExist},
		{from: 2, to: 3, expectErr: os.ErrNotExist},
		{from: 2, to: 4, expectErr: os.ErrNotExist},
		{from: 2, to: 5, expectErr: os.ErrNotExist},
		{from: 2, to: 6, expectErr: os.ErrNotExist},
		{from: 2, to: 7, expectErr: os.ErrNotExist},
		{from: 2, to: 8, expectErr: os.ErrNotExist},

		{from: 3, to: -1, expectErr: nil, expectMigrations: newMigSeq(M(ctx, 3, 1), M(ctx, 1, -1))},
		{from: 3, to: 0, expectErr: os.ErrNotExist},
		{from: 3, to: 1, expectErr: nil, expectMigrations: newMigSeq(M(ctx, 3, 1))},
		{from: 3, to: 2, expectErr: os.ErrNotExist},
		{from: 3, to: 3, expectErr: ErrNoChange},
		{from: 3, to: 4, expectErr: nil, expectMigrations: newMigSeq(M(ctx, 4))},
		{from: 3, to: 5, expectErr: nil, expectMigrations: newMigSeq(M(ctx, 4), M(ctx, 5))},
		{from: 3, to: 6, expectErr: os.ErrNotExist},
		{from: 3, to: 7, expectErr: nil, expectMigrations: newMigSeq(M(ctx, 4), M(ctx, 5), M(ctx, 7))},
		{from: 3, to: 8, expectErr: os.ErrNotExist},

		{from: 4, to: -1, expectErr: nil, expectMigrations: newMigSeq(M(ctx, 4, 3), M(ctx, 3, 1), M(ctx, 1, -1))},
		{from: 4, to: 0, expectErr: os.ErrNotExist},
		{from: 4, to: 1, expectErr: nil, expectMigrations: newMigSeq(M(ctx, 4, 3), M(ctx, 3, 1))},
		{from: 4, to: 2, expectErr: os.ErrNotExist},
		{from: 4, to: 3, expectErr: nil, expectMigrations: newMigSeq(M(ctx, 4, 3))},
		{from: 4, to: 4, expectErr: ErrNoChange},
		{from: 4, to: 5, expectErr: nil, expectMigrations: newMigSeq(M(ctx, 5))},
		{from: 4, to: 6, expectErr: os.ErrNotExist},
		{from: 4, to: 7, expectErr: nil, expectMigrations: newMigSeq(M(ctx, 5), M(ctx, 7))},
		{from: 4, to: 8, expectErr: os.ErrNotExist},

		{from: 5, to: -1, expectErr: nil, expectMigrations: newMigSeq(M(ctx, 5, 4), M(ctx, 4, 3), M(ctx, 3, 1), M(ctx, 1, -1))},
		{from: 5, to: 0, expectErr: os.ErrNotExist},
		{from: 5, to: 1, expectErr: nil, expectMigrations: newMigSeq(M(ctx, 5, 4), M(ctx, 4, 3), M(ctx, 3, 1))},
		{from: 5, to: 2, expectErr: os.ErrNotExist},
		{from: 5, to: 3, expectErr: nil, expectMigrations: newMigSeq(M(ctx, 5, 4), M(ctx, 4, 3))},
		{from: 5, to: 4, expectErr: nil, expectMigrations: newMigSeq(M(ctx, 5, 4))},
		{from: 5, to: 5, expectErr: ErrNoChange},
		{from: 5, to: 6, expectErr: os.ErrNotExist},
		{from: 5, to: 7, expectErr: nil, expectMigrations: newMigSeq(M(ctx, 7))},
		{from: 5, to: 8, expectErr: os.ErrNotExist},

		{from: 6, to: -1, expectErr: os.ErrNotExist},
		{from: 6, to: 0, expectErr: os.ErrNotExist},
		{from: 6, to: 1, expectErr: os.ErrNotExist},
		{from: 6, to: 2, expectErr: os.ErrNotExist},
		{from: 6, to: 3, expectErr: os.ErrNotExist},
		{from: 6, to: 4, expectErr: os.ErrNotExist},
		{from: 6, to: 5, expectErr: os.ErrNotExist},
		{from: 6, to: 6, expectErr: os.ErrNotExist},
		{from: 6, to: 7, expectErr: os.ErrNotExist},
		{from: 6, to: 8, expectErr: os.ErrNotExist},

		{from: 7, to: -1, expectErr: nil, expectMigrations: newMigSeq(M(ctx, 7, 5), M(ctx, 5, 4), M(ctx, 4, 3), M(ctx, 3, 1), M(ctx, 1, -1))},
		{from: 7, to: 0, expectErr: os.ErrNotExist},
		{from: 7, to: 1, expectErr: nil, expectMigrations: newMigSeq(M(ctx, 7, 5), M(ctx, 5, 4), M(ctx, 4, 3), M(ctx, 3, 1))},
		{from: 7, to: 2, expectErr: os.ErrNotExist},
		{from: 7, to: 3, expectErr: nil, expectMigrations: newMigSeq(M(ctx, 7, 5), M(ctx, 5, 4), M(ctx, 4, 3))},
		{from: 7, to: 4, expectErr: nil, expectMigrations: newMigSeq(M(ctx, 7, 5), M(ctx, 5, 4))},
		{from: 7, to: 5, expectErr: nil, expectMigrations: newMigSeq(M(ctx, 7, 5))},
		{from: 7, to: 6, expectErr: os.ErrNotExist},
		{from: 7, to: 7, expectErr: ErrNoChange},
		{from: 7, to: 8, expectErr: os.ErrNotExist},

		{from: 8, to: -1, expectErr: os.ErrNotExist},
		{from: 8, to: 0, expectErr: os.ErrNotExist},
		{from: 8, to: 1, expectErr: os.ErrNotExist},
		{from: 8, to: 2, expectErr: os.ErrNotExist},
		{from: 8, to: 3, expectErr: os.ErrNotExist},
		{from: 8, to: 4, expectErr: os.ErrNotExist},
		{from: 8, to: 5, expectErr: os.ErrNotExist},
		{from: 8, to: 6, expectErr: os.ErrNotExist},
		{from: 8, to: 7, expectErr: os.ErrNotExist},
		{from: 8, to: 8, expectErr: os.ErrNotExist},
	}

	for i, v := range tt {
		ret := make(chan interface{})
		go m.read(ctx, v.from, v.to, ret)
		migrations, err := migrationsFromChannel(ret)

		if (v.expectErr == os.ErrNotExist && !errors.Is(err, os.ErrNotExist)) ||
			(v.expectErr != os.ErrNotExist && v.expectErr != err) {
			t.Errorf("expected %v, got %v, in %v", v.expectErr, err, i)
			t.Logf("%v, in %v", migrations, i)
		}
		if len(v.expectMigrations) > 0 {
			equalMigSeq(t, i, v.expectMigrations, migrations)
		}
	}
}

func TestReadUp(t *testing.T) {
	ctx := context.Background()
	m, _ := New(ctx, "stub://", "stub://")
	m.sourceDrv.(*sStub.Stub).Migrations = sourceStubMigrations

	tt := []struct {
		from             int
		limit            int // -1 means no limit
		expectErr        error
		expectMigrations migrationSequence
	}{
		{from: -1, limit: -1, expectErr: nil, expectMigrations: newMigSeq(M(ctx, 1), M(ctx, 3), M(ctx, 4), M(ctx, 5), M(ctx, 7))},
		{from: -1, limit: 0, expectErr: ErrNoChange},
		{from: -1, limit: 1, expectErr: nil, expectMigrations: newMigSeq(M(ctx, 1))},
		{from: -1, limit: 2, expectErr: nil, expectMigrations: newMigSeq(M(ctx, 1), M(ctx, 3))},

		{from: 0, limit: -1, expectErr: os.ErrNotExist},
		{from: 0, limit: 0, expectErr: os.ErrNotExist},
		{from: 0, limit: 1, expectErr: os.ErrNotExist},
		{from: 0, limit: 2, expectErr: os.ErrNotExist},

		{from: 1, limit: -1, expectErr: nil, expectMigrations: newMigSeq(M(ctx, 3), M(ctx, 4), M(ctx, 5), M(ctx, 7))},
		{from: 1, limit: 0, expectErr: ErrNoChange},
		{from: 1, limit: 1, expectErr: nil, expectMigrations: newMigSeq(M(ctx, 3))},
		{from: 1, limit: 2, expectErr: nil, expectMigrations: newMigSeq(M(ctx, 3), M(ctx, 4))},

		{from: 2, limit: -1, expectErr: os.ErrNotExist},
		{from: 2, limit: 0, expectErr: os.ErrNotExist},
		{from: 2, limit: 1, expectErr: os.ErrNotExist},
		{from: 2, limit: 2, expectErr: os.ErrNotExist},

		{from: 3, limit: -1, expectErr: nil, expectMigrations: newMigSeq(M(ctx, 4), M(ctx, 5), M(ctx, 7))},
		{from: 3, limit: 0, expectErr: ErrNoChange},
		{from: 3, limit: 1, expectErr: nil, expectMigrations: newMigSeq(M(ctx, 4))},
		{from: 3, limit: 2, expectErr: nil, expectMigrations: newMigSeq(M(ctx, 4), M(ctx, 5))},

		{from: 4, limit: -1, expectErr: nil, expectMigrations: newMigSeq(M(ctx, 5), M(ctx, 7))},
		{from: 4, limit: 0, expectErr: ErrNoChange},
		{from: 4, limit: 1, expectErr: nil, expectMigrations: newMigSeq(M(ctx, 5))},
		{from: 4, limit: 2, expectErr: nil, expectMigrations: newMigSeq(M(ctx, 5), M(ctx, 7))},

		{from: 5, limit: -1, expectErr: nil, expectMigrations: newMigSeq(M(ctx, 7))},
		{from: 5, limit: 0, expectErr: ErrNoChange},
		{from: 5, limit: 1, expectErr: nil, expectMigrations: newMigSeq(M(ctx, 7))},
		{from: 5, limit: 2, expectErr: ErrShortLimit{1}, expectMigrations: newMigSeq(M(ctx, 7))},

		{from: 6, limit: -1, expectErr: os.ErrNotExist},
		{from: 6, limit: 0, expectErr: os.ErrNotExist},
		{from: 6, limit: 1, expectErr: os.ErrNotExist},
		{from: 6, limit: 2, expectErr: os.ErrNotExist},

		{from: 7, limit: -1, expectErr: ErrNoChange},
		{from: 7, limit: 0, expectErr: ErrNoChange},
		{from: 7, limit: 1, expectErr: os.ErrNotExist},
		{from: 7, limit: 2, expectErr: os.ErrNotExist},

		{from: 8, limit: -1, expectErr: os.ErrNotExist},
		{from: 8, limit: 0, expectErr: os.ErrNotExist},
		{from: 8, limit: 1, expectErr: os.ErrNotExist},
		{from: 8, limit: 2, expectErr: os.ErrNotExist},
	}

	for i, v := range tt {
		ret := make(chan interface{})
		go m.readUp(ctx, v.from, v.limit, ret)
		migrations, err := migrationsFromChannel(ret)

		if (v.expectErr == os.ErrNotExist && !errors.Is(err, os.ErrNotExist)) ||
			(v.expectErr != os.ErrNotExist && v.expectErr != err) {
			t.Errorf("expected %v, got %v, in %v", v.expectErr, err, i)
			t.Logf("%v, in %v", migrations, i)
		}
		if len(v.expectMigrations) > 0 {
			equalMigSeq(t, i, v.expectMigrations, migrations)
		}
	}
}

func TestReadDown(t *testing.T) {
	ctx := context.Background()
	m, _ := New(ctx, "stub://", "stub://")
	m.sourceDrv.(*sStub.Stub).Migrations = sourceStubMigrations

	tt := []struct {
		from             int
		limit            int // -1 means no limit
		expectErr        error
		expectMigrations migrationSequence
	}{
		{from: -1, limit: -1, expectErr: ErrNoChange},
		{from: -1, limit: 0, expectErr: ErrNoChange},
		{from: -1, limit: 1, expectErr: os.ErrNotExist},
		{from: -1, limit: 2, expectErr: os.ErrNotExist},

		{from: 0, limit: -1, expectErr: os.ErrNotExist},
		{from: 0, limit: 0, expectErr: os.ErrNotExist},
		{from: 0, limit: 1, expectErr: os.ErrNotExist},
		{from: 0, limit: 2, expectErr: os.ErrNotExist},

		{from: 1, limit: -1, expectErr: nil, expectMigrations: newMigSeq(M(ctx, 1, -1))},
		{from: 1, limit: 0, expectErr: ErrNoChange},
		{from: 1, limit: 1, expectErr: nil, expectMigrations: newMigSeq(M(ctx, 1, -1))},
		{from: 1, limit: 2, expectErr: ErrShortLimit{1}, expectMigrations: newMigSeq(M(ctx, 1, -1))},

		{from: 2, limit: -1, expectErr: os.ErrNotExist},
		{from: 2, limit: 0, expectErr: os.ErrNotExist},
		{from: 2, limit: 1, expectErr: os.ErrNotExist},
		{from: 2, limit: 2, expectErr: os.ErrNotExist},

		{from: 3, limit: -1, expectErr: nil, expectMigrations: newMigSeq(M(ctx, 3, 1), M(ctx, 1, -1))},
		{from: 3, limit: 0, expectErr: ErrNoChange},
		{from: 3, limit: 1, expectErr: nil, expectMigrations: newMigSeq(M(ctx, 3, 1))},
		{from: 3, limit: 2, expectErr: nil, expectMigrations: newMigSeq(M(ctx, 3, 1), M(ctx, 1, -1))},

		{from: 4, limit: -1, expectErr: nil, expectMigrations: newMigSeq(M(ctx, 4, 3), M(ctx, 3, 1), M(ctx, 1, -1))},
		{from: 4, limit: 0, expectErr: ErrNoChange},
		{from: 4, limit: 1, expectErr: nil, expectMigrations: newMigSeq(M(ctx, 4, 3))},
		{from: 4, limit: 2, expectErr: nil, expectMigrations: newMigSeq(M(ctx, 4, 3), M(ctx, 3, 1))},

		{from: 5, limit: -1, expectErr: nil, expectMigrations: newMigSeq(M(ctx, 5, 4), M(ctx, 4, 3), M(ctx, 3, 1), M(ctx, 1, -1))},
		{from: 5, limit: 0, expectErr: ErrNoChange},
		{from: 5, limit: 1, expectErr: nil, expectMigrations: newMigSeq(M(ctx, 5, 4))},
		{from: 5, limit: 2, expectErr: nil, expectMigrations: newMigSeq(M(ctx, 5, 4), M(ctx, 4, 3))},

		{from: 6, limit: -1, expectErr: os.ErrNotExist},
		{from: 6, limit: 0, expectErr: os.ErrNotExist},
		{from: 6, limit: 1, expectErr: os.ErrNotExist},
		{from: 6, limit: 2, expectErr: os.ErrNotExist},

		{from: 7, limit: -1, expectErr: nil, expectMigrations: newMigSeq(M(ctx, 7, 5), M(ctx, 5, 4), M(ctx, 4, 3), M(ctx, 3, 1), M(ctx, 1, -1))},
		{from: 7, limit: 0, expectErr: ErrNoChange},
		{from: 7, limit: 1, expectErr: nil, expectMigrations: newMigSeq(M(ctx, 7, 5))},
		{from: 7, limit: 2, expectErr: nil, expectMigrations: newMigSeq(M(ctx, 7, 5), M(ctx, 5, 4))},

		{from: 8, limit: -1, expectErr: os.ErrNotExist},
		{from: 8, limit: 0, expectErr: os.ErrNotExist},
		{from: 8, limit: 1, expectErr: os.ErrNotExist},
		{from: 8, limit: 2, expectErr: os.ErrNotExist},
	}

	for i, v := range tt {
		ret := make(chan interface{})
		go m.readDown(ctx, v.from, v.limit, ret)
		migrations, err := migrationsFromChannel(ret)

		if (v.expectErr == os.ErrNotExist && !errors.Is(err, os.ErrNotExist)) ||
			(v.expectErr != os.ErrNotExist && v.expectErr != err) {
			t.Errorf("expected %v, got %v, in %v", v.expectErr, err, i)
			t.Logf("%v, in %v", migrations, i)
		}
		if len(v.expectMigrations) > 0 {
			equalMigSeq(t, i, v.expectMigrations, migrations)
		}
	}
}

func TestLock(t *testing.T) {
	ctx := context.Background()
	m, _ := New(ctx, "stub://", "stub://")
	if err := m.lock(ctx); err != nil {
		t.Fatal(err)
	}

	if err := m.lock(ctx); err == nil {
		t.Fatal("should be locked already")
	}
}

func migrationsFromChannel(ret chan interface{}) ([]*Migration, error) {
	slice := make([]*Migration, 0)
	for r := range ret {
		switch t := r.(type) {
		case error:
			return slice, t

		case *Migration:
			slice = append(slice, t)
		}
	}
	return slice, nil
}

type migrationSequence []*Migration

func newMigSeq(migr ...*Migration) migrationSequence {
	return migr
}

func (m *migrationSequence) add(migr ...*Migration) migrationSequence { // nolint:unused
	*m = append(*m, migr...)
	return *m
}

func (m *migrationSequence) bodySequence() []string {
	r := make([]string, 0)
	for _, v := range *m {
		if v.Body != nil {
			body, err := io.ReadAll(v.Body)
			if err != nil {
				panic(err) // that should never happen
			}

			// reset body reader
			// TODO: is there a better/nicer way?
			v.Body = io.NopCloser(bytes.NewReader(body))

			r = append(r, string(body[:]))
		} else {
			r = append(r, "<empty>")
		}
	}
	return r
}

// M is a convenience func to create a new *Migration
func M(ctx context.Context, version uint, targetVersion ...int) *Migration {
	if len(targetVersion) > 1 {
		panic("only one targetVersion allowed")
	}
	ts := int(version)
	if len(targetVersion) == 1 {
		ts = targetVersion[0]
	}

	m, _ := New(ctx, "stub://", "stub://")
	m.sourceDrv.(*sStub.Stub).Migrations = sourceStubMigrations
	migr, err := m.newMigration(ctx, version, ts)
	if err != nil {
		panic(err)
	}
	return migr
}

// mr is a convenience func to create a new *Migration from the raw database query
func mr(value string) *Migration {
	return &Migration{
		Body: io.NopCloser(strings.NewReader(value)),
	}
}

func equalMigSeq(t *testing.T, i int, expected, got migrationSequence) {
	if len(expected) != len(got) {
		t.Errorf("expected migrations %v, got %v, in %v", expected, got, i)

	} else {
		for ii := 0; ii < len(expected); ii++ {
			if expected[ii].Version != got[ii].Version {
				t.Errorf("expected version %v, got %v, in %v", expected[ii].Version, got[ii].Version, i)
			}

			if expected[ii].TargetVersion != got[ii].TargetVersion {
				t.Errorf("expected targetVersion %v, got %v, in %v", expected[ii].TargetVersion, got[ii].TargetVersion, i)
			}
		}
	}
}

func equalDbSeq(t *testing.T, i int, expected migrationSequence, got *dStub.Stub) {
	bs := expected.bodySequence()
	if !got.EqualSequence(bs) {
		t.Fatalf("\nexpected sequence %v,\ngot               %v, in %v", bs, got.MigrationSequence, i)
	}
}
