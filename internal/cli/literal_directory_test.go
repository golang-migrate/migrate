package cli

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestCreateLiteralDirectory(t *testing.T) {
	for _, name := range []string{"plain", "migrations[dev]", "migrations[", "migrations*", "migrations?", `migrations\dev[local]`} {
		t.Run(name, func(t *testing.T) {
			if runtime.GOOS == "windows" && strings.ContainsAny(name, `*?\`) {
				t.Skip("not a valid literal Windows directory name")
			}
			dir := filepath.Join(t.TempDir(), name)
			// A similarly named sibling must not contribute versions to this directory.
			if strings.ContainsAny(name, "*?[") {
				sibling := filepath.Join(filepath.Dir(dir), strings.NewReplacer("*", "prod", "?", "p", "[dev]", "d", "[", "x").Replace(name))
				if err := os.MkdirAll(sibling, 0755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(sibling, "000009_other.up.sql"), nil, 0644); err != nil {
					t.Fatal(err)
				}
			}
			for _, migration := range []string{"first", "second"} {
				if err := createCmd(dir, time.Time{}, defaultTimeFormat, migration, "sql", true, 6, false); err != nil {
					t.Fatalf("create %s: %v", migration, err)
				}
			}
			entries, err := os.ReadDir(dir)
			if err != nil {
				t.Fatal(err)
			}
			want := []string{"000001_first.down.sql", "000001_first.up.sql", "000002_second.down.sql", "000002_second.up.sql"}
			if len(entries) != len(want) {
				t.Fatalf("entries: %v", entries)
			}
			for i, entry := range entries {
				if entry.Name() != want[i] {
					t.Errorf("entry %d: got %q want %q", i, entry.Name(), want[i])
				}
			}
		})
	}
}

func TestCreateLiteralDirectoryDuplicate(t *testing.T) {
	for _, name := range []string{"plain", "migrations[dev]"} {
		t.Run(name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), name)
			ts := time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)
			if err := createCmd(dir, ts, defaultTimeFormat, "first", "sql", false, 6, false); err != nil {
				t.Fatal(err)
			}
			err := createCmd(dir, ts, defaultTimeFormat, "second", "sql", false, 6, false)
			if err == nil || !strings.Contains(err.Error(), "duplicate migration version") {
				t.Fatalf("duplicate create: %v", err)
			}
			entries, err := os.ReadDir(dir)
			if err != nil {
				t.Fatal(err)
			}
			if len(entries) != 2 {
				t.Fatalf("duplicate created extra files: %v", entries)
			}
		})
	}
}
