package migrate

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	dStub "github.com/golang-migrate/migrate/v4/database/stub"
	sStub "github.com/golang-migrate/migrate/v4/source/stub"
)

func setupMigrateInstance(tempDir string) (*Migrate, *dStub.Stub) {
	scheme := "stub://"
	m, _ := New(scheme, scheme)
	m.dirtyStateConf = &dirtyStateConfig{
		destScheme: scheme,
		destPath:   tempDir,
		enable:     true,
	}
	return m, m.databaseDrv.(*dStub.Stub)
}

func TestHandleDirtyState(t *testing.T) {
	tempDir := t.TempDir()

	m, dbDrv := setupMigrateInstance(tempDir)
	m.sourceDrv.(*sStub.Stub).Migrations = sourceStubMigrations

	tests := []struct {
		lastSuccessfulVersion int
		currentVersion        int
		err                   error
		setupFailure          bool
	}{
		{lastSuccessfulVersion: 1, currentVersion: 3, err: nil, setupFailure: false},
		{lastSuccessfulVersion: 4, currentVersion: 7, err: nil, setupFailure: false},
		{lastSuccessfulVersion: 3, currentVersion: 4, err: nil, setupFailure: false},
		{lastSuccessfulVersion: -3, currentVersion: 4, err: ErrInvalidVersion, setupFailure: false},
		{lastSuccessfulVersion: 4, currentVersion: 3, err: fmt.Errorf("open %s: no such file or directory", filepath.Join(tempDir, lastSuccessfulMigrationFile)), setupFailure: true},
	}

	for _, test := range tests {
		t.Run("", func(t *testing.T) {
			var lastSuccessfulMigrationPath string
			// setupFailure flag helps with testing scenario where the 'lastSuccessfulMigrationFile' doesn't exist
			if !test.setupFailure {
				lastSuccessfulMigrationPath = filepath.Join(tempDir, lastSuccessfulMigrationFile)
				if err := os.WriteFile(lastSuccessfulMigrationPath, []byte(strconv.Itoa(test.lastSuccessfulVersion)), 0644); err != nil {
					t.Fatal(err)
				}
			}
			// Setting the DB version as dirty
			if err := dbDrv.SetVersion(test.currentVersion, true); err != nil {
				t.Fatal(err)
			}
			// Quick check to see if set correctly
			version, b, err := dbDrv.Version()
			if err != nil {
				t.Fatal(err)
			}
			if version != test.currentVersion {
				t.Fatalf("expected version %d, got %d", test.currentVersion, version)
			}

			if !b {
				t.Fatalf("expected DB to be dirty, got false")
			}

			// Handle dirty state
			if err = m.handleDirtyState(); err != nil {
				if strings.Contains(err.Error(), test.err.Error()) {
					t.Logf("expected error %v, got %v", test.err, err)
					if !test.setupFailure {
						if err = os.Remove(lastSuccessfulMigrationPath); err != nil {
							t.Fatal(err)
						}
					}
					return
				} else {
					t.Fatal(err)
				}
			}
			// Check 1: DB should no longer be dirty
			if dbDrv.IsDirty {
				t.Fatalf("expected dirty to be false, got true")
			}
			// Check 2: Current version should be the last successful version
			if dbDrv.CurrentVersion != test.lastSuccessfulVersion {
				t.Fatalf("expected version %d, got %d", test.lastSuccessfulVersion, dbDrv.CurrentVersion)
			}
			// Check 3: The lastSuccessfulMigration file shouldn't exist
			if _, err = os.Stat(lastSuccessfulMigrationPath); !os.IsNotExist(err) {
				t.Fatalf("expected file to be deleted, but it still exists")
			}
		})
	}
}

func TestHandleMigrationFailure(t *testing.T) {
	tempDir := t.TempDir()

	m, _ := setupMigrateInstance(tempDir)

	tests := []struct {
		lastSuccessFulVersion int
	}{
		{lastSuccessFulVersion: 3},
		{lastSuccessFulVersion: 4},
		{lastSuccessFulVersion: 5},
	}

	for _, test := range tests {
		t.Run("", func(t *testing.T) {
			if err := m.handleMigrationFailure(test.lastSuccessFulVersion); err != nil {
				t.Fatal(err)
			}
			// Check 1: last successful Migration version should be stored in a file
			lastSuccessfulMigrationPath := filepath.Join(tempDir, lastSuccessfulMigrationFile)
			if _, err := os.Stat(lastSuccessfulMigrationPath); os.IsNotExist(err) {
				t.Fatalf("expected file to be created, but it does not exist")
			}

			// Check 2: Check if the content of last successful migration has the correct version
			content, err := os.ReadFile(lastSuccessfulMigrationPath)
			if err != nil {
				t.Fatal(err)
			}

			if string(content) != strconv.Itoa(test.lastSuccessFulVersion) {
				t.Fatalf("expected %d, got %s", test.lastSuccessFulVersion, string(content))
			}
		})
	}
}

func TestCleanupFiles(t *testing.T) {
	tempDir := t.TempDir()

	m, _ := setupMigrateInstance(tempDir)
	m.sourceDrv.(*sStub.Stub).Migrations = sourceStubMigrations

	tests := []struct {
		migrationFiles []string
		targetVersion  uint
		remainingFiles []string
		emptyDestPath  bool
	}{
		{
			migrationFiles: []string{"1_name.up.sql", "2_name.up.sql", "3_name.up.sql"},
			targetVersion:  2,
			remainingFiles: []string{"1_name.up.sql", "2_name.up.sql"},
		},
		{
			migrationFiles: []string{"1_name.up.sql", "2_name.up.sql", "3_name.up.sql", "4_name.up.sql", "5_name.up.sql"},
			targetVersion:  3,
			remainingFiles: []string{"1_name.up.sql", "2_name.up.sql", "3_name.up.sql"},
		},
		{
			migrationFiles: []string{},
			targetVersion:  1,
			remainingFiles: []string{},
			emptyDestPath:  true,
		},
	}

	for _, test := range tests {
		t.Run("", func(t *testing.T) {
			for _, file := range test.migrationFiles {
				if err := os.WriteFile(filepath.Join(tempDir, file), []byte(""), 0644); err != nil {
					t.Fatal(err)
				}
			}

			if test.emptyDestPath {
				m.dirtyStateConf.destPath = ""
			}

			if err := m.cleanupFiles(test.targetVersion); err != nil {
				t.Fatal(err)
			}
			// check 1: only files upto the target version should exist
			for _, file := range test.remainingFiles {
				if _, err := os.Stat(filepath.Join(tempDir, file)); os.IsNotExist(err) {
					t.Fatalf("expected file %s to exist, but it does not", file)
				}
			}

			// check 2: the files removed are as expected
			deletedFiles := diff(test.migrationFiles, test.remainingFiles)
			for _, deletedFile := range deletedFiles {
				if _, err := os.Stat(filepath.Join(tempDir, deletedFile)); !os.IsNotExist(err) {
					t.Fatalf("expected file %s to be deleted, but it still exists", deletedFile)
				}
			}
		})
	}
}

func TestCopyFiles(t *testing.T) {
	srcDir := t.TempDir()
	destDir := t.TempDir()

	m, _ := setupMigrateInstance(destDir)
	m.dirtyStateConf.srcPath = srcDir

	tests := []struct {
		migrationFiles []string
		copiedFiles    []string
		emptyDestPath  bool
	}{
		{
			migrationFiles: []string{"1_name.up.sql", "2_name.up.sql", "3_name.up.sql"},
			copiedFiles:    []string{"1_name.up.sql", "2_name.up.sql", "3_name.up.sql"},
		},
		{
			migrationFiles: []string{"1_name.up.sql", "2_name.up.sql", "3_name.up.sql", "4_name.up.sql", "current.sql"},
			copiedFiles:    []string{"1_name.up.sql", "2_name.up.sql", "3_name.up.sql", "4_name.up.sql"},
		},
		{
			emptyDestPath: true, // copyFiles should not do anything
		},
	}

	for _, test := range tests {
		t.Run("", func(t *testing.T) {
			for _, file := range test.migrationFiles {
				if err := os.WriteFile(filepath.Join(srcDir, file), []byte(""), 0644); err != nil {
					t.Fatal(err)
				}
			}
			if test.emptyDestPath {
				m.dirtyStateConf.destPath = ""
			}

			if err := m.copyFiles(); err != nil {
				t.Fatal(err)
			}

			for _, file := range test.copiedFiles {
				if _, err := os.Stat(filepath.Join(destDir, file)); os.IsNotExist(err) {
					t.Fatalf("expected file %s to be copied, but it does not exist", file)
				}
			}
		})
	}
}

func TestWithDirtyStateConfig(t *testing.T) {
	tests := []struct {
		name     string
		srcPath  string
		destPath string
		isDirty  bool
		wantErr  bool
		wantConf *dirtyStateConfig
	}{
		{
			name:     "Valid file paths",
			srcPath:  "file:///src/path",
			destPath: "file:///dest/path",
			isDirty:  true,
			wantErr:  false,
			wantConf: &dirtyStateConfig{
				srcScheme:  "file://",
				destScheme: "file://",
				srcPath:    "/src/path",
				destPath:   "/dest/path",
				enable:     true,
			},
		},
		{
			name:     "Invalid source scheme",
			srcPath:  "s3:///src/path",
			destPath: "file:///dest/path",
			isDirty:  true,
			wantErr:  true,
		},
		{
			name:     "Invalid destination scheme",
			srcPath:  "file:///src/path",
			destPath: "s3:///dest/path",
			isDirty:  true,
			wantErr:  true,
		},
		{
			name:     "Empty source scheme",
			srcPath:  "/src/path",
			destPath: "file:///dest/path",
			isDirty:  true,
			wantErr:  false,
			wantConf: &dirtyStateConfig{
				srcScheme:  "file://",
				destScheme: "file://",
				srcPath:    "/src/path",
				destPath:   "/dest/path",
				enable:     true,
			},
		},
		{
			name:     "Empty destination scheme",
			srcPath:  "file:///src/path",
			destPath: "/dest/path",
			isDirty:  true,
			wantErr:  false,
			wantConf: &dirtyStateConfig{
				srcScheme:  "file://",
				destScheme: "file://",
				srcPath:    "/src/path",
				destPath:   "/dest/path",
				enable:     true,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &Migrate{}
			err := m.WithDirtyStateConfig(tt.srcPath, tt.destPath, tt.isDirty)
			if (err != nil) != tt.wantErr {
				t.Errorf("error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && m.dirtyStateConf == tt.wantConf {
				t.Errorf("dirtyStateConf = %v, want %v", m.dirtyStateConf, tt.wantConf)
			}
		})
	}
}

/*
   diff returns an array containing the elements in Array A and not in B
*/

func diff(a, b []string) []string {
	temp := map[string]int{}
	for _, s := range a {
		temp[s]++
	}
	for _, s := range b {
		temp[s]--
	}

	var result []string
	for s, v := range temp {
		if v != 0 {
			result = append(result, s)
		}
	}
	return result
}
