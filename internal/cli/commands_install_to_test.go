package cli

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestInstallToCmdSuccess(t *testing.T) {
	// Create a temporary directory for testing
	tempDir := t.TempDir()
	destDir := filepath.Join(tempDir, "success")
	err := os.MkdirAll(destDir, 0755)
	if err != nil {
		t.Fatalf("Failed to create test directory: %v", err)
	}

	// Run the install command
	err = installToCmd(destDir)
	if err != nil {
		t.Fatalf("installToCmd failed: %v", err)
	}

	// Get the current executable name
	executablePath, err := os.Executable()
	if err != nil {
		t.Fatalf("Failed to get executable path: %v", err)
	}
	executableName := filepath.Base(executablePath)
	
	// Check that the file was created
	installedPath := filepath.Join(destDir, executableName)
	info, err := os.Stat(installedPath)
	if err != nil {
		t.Fatalf("Installed binary not found: %v", err)
	}

	// Check that it's executable (has execute permission)
	if info.Mode().Perm()&0111 == 0 {
		t.Error("Installed binary is not executable")
	}

	// Check that the file size matches the original
	originalInfo, err := os.Stat(executablePath)
	if err != nil {
		t.Fatalf("Failed to stat original executable: %v", err)
	}

	if info.Size() != originalInfo.Size() {
		t.Errorf("Installed binary size (%d) doesn't match original (%d)", info.Size(), originalInfo.Size())
	}

	// Check that permissions are preserved
	if info.Mode().Perm() != originalInfo.Mode().Perm() {
		t.Errorf("Permissions not preserved: got %v, want %v", info.Mode().Perm(), originalInfo.Mode().Perm())
	}
}

func TestInstallToCmdNonexistentDirectory(t *testing.T) {
	tempDir := t.TempDir()
	nonexistentDir := filepath.Join(tempDir, "nonexistent", "deep", "path")
	
	err := installToCmd(nonexistentDir)
	if err == nil {
		t.Error("Expected error for nonexistent directory, got nil")
	}
	
	// Should contain a meaningful error message
	if err != nil && err.Error() == "" {
		t.Error("Error message should not be empty")
	}
}

func TestInstallToCmdDestinationIsFile(t *testing.T) {
	tempDir := t.TempDir()
	// Create a file instead of directory
	filePath := filepath.Join(tempDir, "notadir")
	file, err := os.Create(filePath)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}
	file.Close()

	err = installToCmd(filePath)
	if err == nil {
		t.Error("Expected error when destination is a file, got nil")
	}
}

func TestInstallToCmdOverwriteExisting(t *testing.T) {
	tempDir := t.TempDir()
	destDir := filepath.Join(tempDir, "overwrite")
	err := os.MkdirAll(destDir, 0755)
	if err != nil {
		t.Fatalf("Failed to create test directory: %v", err)
	}

	// Get the current executable name
	executablePath, err := os.Executable()
	if err != nil {
		t.Fatalf("Failed to get executable path: %v", err)
	}
	executableName := filepath.Base(executablePath)
	existingPath := filepath.Join(destDir, executableName)

	// Create a dummy file to overwrite
	dummyContent := []byte("dummy content")
	err = os.WriteFile(existingPath, dummyContent, 0644)
	if err != nil {
		t.Fatalf("Failed to create dummy file: %v", err)
	}

	// Run the install command
	err = installToCmd(destDir)
	if err != nil {
		t.Fatalf("installToCmd failed: %v", err)
	}

	// Check that the file was overwritten
	installedContent, err := os.ReadFile(existingPath)
	if err != nil {
		t.Fatalf("Failed to read installed binary: %v", err)
	}

	// Should not be the dummy content anymore
	if string(installedContent) == string(dummyContent) {
		t.Error("File was not overwritten - still contains dummy content")
	}

	// Check that it's executable
	info, err := os.Stat(existingPath)
	if err != nil {
		t.Fatalf("Failed to stat installed binary: %v", err)
	}

	if info.Mode().Perm()&0111 == 0 {
		t.Error("Overwritten binary is not executable")
	}
}

func TestInstallToCmdTempFileCleanup(t *testing.T) {
	tempDir := t.TempDir()
	destDir := filepath.Join(tempDir, "cleanup")
	err := os.MkdirAll(destDir, 0755)
	if err != nil {
		t.Fatalf("Failed to create test directory: %v", err)
	}

	// Get the current executable name
	executablePath, err := os.Executable()
	if err != nil {
		t.Fatalf("Failed to get executable path: %v", err)
	}
	executableName := filepath.Base(executablePath)
	
	// We can't easily simulate a failure in the middle of installToCmd,
	// but we can check that no temp files are left after successful execution
	err = installToCmd(destDir)
	if err != nil {
		t.Fatalf("installToCmd failed: %v", err)
	}

	// Check that no .tmp files are left behind
	tempPattern := filepath.Join(destDir, executableName+".tmp")
	matches, err := filepath.Glob(tempPattern)
	if err != nil {
		t.Fatalf("Failed to glob for temp files: %v", err)
	}

	if len(matches) > 0 {
		t.Errorf("Temp files left behind: %v", matches)
	}

	// Also check for any .tmp files in the directory
	entries, err := os.ReadDir(destDir)
	if err != nil {
		t.Fatalf("Failed to read directory: %v", err)
	}

	for _, entry := range entries {
		if filepath.Ext(entry.Name()) == ".tmp" {
			t.Errorf("Temp file left behind: %s", entry.Name())
		}
	}
}

func TestInstallToCmdPreservesOriginal(t *testing.T) {
	tempDir := t.TempDir()
	destDir := filepath.Join(tempDir, "preserve")
	err := os.MkdirAll(destDir, 0755)
	if err != nil {
		t.Fatalf("Failed to create test directory: %v", err)
	}

	// Get original file info before copy
	executablePath, err := os.Executable()
	if err != nil {
		t.Fatalf("Failed to get executable path: %v", err)
	}

	originalInfo, err := os.Stat(executablePath)
	if err != nil {
		t.Fatalf("Failed to stat original file: %v", err)
	}

	// Run the install command
	err = installToCmd(destDir)
	if err != nil {
		t.Fatalf("installToCmd failed: %v", err)
	}

	// Check that original file is unchanged
	afterInfo, err := os.Stat(executablePath)
	if err != nil {
		t.Fatalf("Failed to stat original file after copy: %v", err)
	}

	if originalInfo.Size() != afterInfo.Size() {
		t.Error("Original file size changed during copy")
	}

	if originalInfo.ModTime() != afterInfo.ModTime() {
		t.Error("Original file modification time changed during copy")
	}

	if originalInfo.Mode() != afterInfo.Mode() {
		t.Error("Original file mode changed during copy")
	}
}

// TestInstallToCmdIntegration tests the command through the CLI interface
func TestInstallToCmdIntegration(t *testing.T) {
	// Build a test binary first
	tempDir := t.TempDir()
	testBinary := filepath.Join(tempDir, "migrate-test")
	
	// Find the repo root by looking for go.mod
	repoRoot := "."
	for i := 0; i < 5; i++ { // Look up to 5 levels up
		if _, err := os.Stat(filepath.Join(repoRoot, "go.mod")); err == nil {
			break
		}
		repoRoot = filepath.Join("..", repoRoot)
	}
	
	cmd := exec.Command("go", "build", "-o", testBinary, "./cmd/migrate")
	cmd.Dir = repoRoot
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Failed to build test binary: %v, output: %s", err, output)
	}

	t.Run("successful installation via CLI", func(t *testing.T) {
		destDir := filepath.Join(tempDir, "cli-success")
		err := os.MkdirAll(destDir, 0755)
		if err != nil {
			t.Fatalf("Failed to create destination directory: %v", err)
		}

		cmd := exec.Command(testBinary, "install-to", destDir)
		output, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("Command failed: %v, output: %s", err, output)
		}

		// Check that the binary was installed
		installedPath := filepath.Join(destDir, "migrate-test")
		if _, err := os.Stat(installedPath); err != nil {
			t.Fatalf("Installed binary not found: %v", err)
		}

		// Check that output contains success message
		outputStr := string(output)
		if !strings.Contains(outputStr, "Binary successfully installed") {
			t.Errorf("Expected success message in output, got: %s", outputStr)
		}
	})

	t.Run("error when no directory specified", func(t *testing.T) {
		cmd := exec.Command(testBinary, "install-to")
		output, err := cmd.CombinedOutput()
		if err == nil {
			t.Error("Expected command to fail when no directory specified")
		}

		// Check that it exits with non-zero code
		if exitError, ok := err.(*exec.ExitError); ok {
			if exitError.ExitCode() == 0 {
				t.Error("Expected non-zero exit code")
			}
		}

		// Check error message
		outputStr := string(output)
		if !strings.Contains(outputStr, "please specify destination directory") {
			t.Errorf("Expected error message in output, got: %s", outputStr)
		}
	})

	t.Run("error when directory doesn't exist", func(t *testing.T) {
		nonexistentDir := filepath.Join(tempDir, "nonexistent")
		cmd := exec.Command(testBinary, "install-to", nonexistentDir)
		output, err := cmd.CombinedOutput()
		if err == nil {
			t.Error("Expected command to fail for nonexistent directory")
		}

		// Check that it exits with non-zero code
		if exitError, ok := err.(*exec.ExitError); ok {
			if exitError.ExitCode() == 0 {
				t.Error("Expected non-zero exit code")
			}
		}

		// Check error message
		outputStr := string(output)
		if !strings.Contains(outputStr, "destination directory does not exist") {
			t.Errorf("Expected error message in output, got: %s", outputStr)
		}
	})

	t.Run("help message", func(t *testing.T) {
		cmd := exec.Command(testBinary, "install-to", "--help")
		output, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("Help command failed: %v", err)
		}

		outputStr := string(output)
		if !strings.Contains(outputStr, "install-to DIR") {
			t.Errorf("Expected install-to command in help output, got: %s", outputStr)
		}
		if !strings.Contains(outputStr, "Copy the running binary to the specified directory") {
			t.Errorf("Expected install-to description in help output, got: %s", outputStr)
		}
	})
}
