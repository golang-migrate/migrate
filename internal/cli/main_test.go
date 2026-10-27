package cli

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

func TestMainDatabaseEnv(t *testing.T) {
	// Main calls os.Exit, so exercise it in a subprocess of the test binary.
	if os.Getenv("MIGRATE_TEST_CLI") == "1" {
		for i, arg := range os.Args {
			if arg == "--" {
				os.Args = append([]string{"migrate"}, os.Args[i+1:]...)
				Main("test-version")
				return
			}
		}
		t.Fatal("missing CLI arguments")
	}

	const envName = "MIGRATE_TEST_DATABASE_URL"
	const databaseURL = "stub://user:test-password@localhost/database"
	tests := []struct {
		name     string
		value    string
		unset    bool
		args     []string
		wantCode int
		wantText string
	}{
		{name: "environment URL", value: databaseURL, args: []string{"-database-env", envName, "force", "7"}},
		{name: "explicit URL", value: "invalid", args: []string{"-database", databaseURL, "force", "7"}},
		{name: "conflicting flags", value: databaseURL, args: []string{"-database", databaseURL, "-database-env", envName, "force", "7"}, wantCode: 1, wantText: "mutually exclusive"},
		{name: "unset variable", unset: true, args: []string{"-database-env", envName, "force", "7"}, wantCode: 1, wantText: "is not set or is empty"},
		{name: "empty variable", args: []string{"-database-env", envName, "force", "7"}, wantCode: 1, wantText: "is not set or is empty"},
		{name: "no implicit fallback", value: databaseURL, args: []string{"force", "7"}, wantCode: 1, wantText: "URL cannot be empty"},
		{name: "help without variable", unset: true, args: []string{"-database-env", envName, "-help"}, wantText: "-database-env"},
		{name: "help with secret", value: databaseURL, args: []string{"-database-env", envName, "-help"}, wantText: "-database-env"},
		{name: "version without variable", unset: true, args: []string{"-database-env", envName, "-version"}, wantText: "test-version"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("MIGRATE_TEST_CLI", "1")
			t.Setenv(envName, tt.value)
			if tt.unset {
				if err := os.Unsetenv(envName); err != nil {
					t.Fatal(err)
				}
			}
			args := []string{"-test.run=^TestMainDatabaseEnv$", "--", "-path", "."}
			cmd := exec.Command(os.Args[0], append(args, tt.args...)...)
			cmd.Dir = t.TempDir()
			output, err := cmd.CombinedOutput()
			code := 0
			if err != nil {
				if exitErr, ok := err.(*exec.ExitError); ok {
					code = exitErr.ExitCode()
				} else {
					t.Fatal(err)
				}
			}
			if code != tt.wantCode || !strings.Contains(string(output), tt.wantText) {
				t.Fatalf("exit %d, want %d; output: %s; want text %q", code, tt.wantCode, output, tt.wantText)
			}
			if strings.Contains(string(output), "test-password") {
				t.Fatal("CLI output exposed the database password")
			}
		})
	}
}
