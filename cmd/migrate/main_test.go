package main

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

func TestCLIFunctionality(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	
	// Test that the CLI can be built
	buildCmd := exec.Command("go", "build", "-o", "migrate-test")
	buildCmd.Env = os.Environ()
	output, err := buildCmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Failed to build CLI: %v\nOutput: %s", err, output)
	}
	defer os.Remove("migrate-test")
	
	// Test the version command
	versionCmd := exec.Command("./migrate-test", "-version")
	output, err = versionCmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Failed to run version command: %v\nOutput: %s", err, output)
	}
	
	if !strings.Contains(string(output), "migrate version") {
		t.Errorf("Expected version output, got: %s", output)
	}
	
	// Test the help command
	helpCmd := exec.Command("./migrate-test", "-help")
	output, err = helpCmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Failed to run help command: %v\nOutput: %s", err, output)
	}
	
	if !strings.Contains(string(output), "Usage:") || !strings.Contains(string(output), "Commands:") {
		t.Errorf("Expected help output, got: %s", output)
	}
}
