package cli

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

func installToCmd(destDir string) error {
	// Get the path to the current executable
	executablePath, err := os.Executable()
	if err != nil {
		return fmt.Errorf("failed to get executable path: %w", err)
	}

	// Get the base name of the executable
	executableName := filepath.Base(executablePath)

	// Create destination path
	destPath := filepath.Join(destDir, executableName)
	tempPath := destPath + ".tmp"

	// Remove temp file on error
	defer func() {
		if err != nil {
			if _, statErr := os.Stat(tempPath); statErr == nil {
				os.Remove(tempPath)
			}
		}
	}()

	// Get source file info to preserve permissions
	sourceInfo, err := os.Stat(executablePath)
	if err != nil {
		return fmt.Errorf("failed to get source file info: %w", err)
	}

	// Open source file
	sourceFile, err := os.Open(executablePath)
	if err != nil {
		return fmt.Errorf("failed to open source file: %w", err)
	}
	defer sourceFile.Close()

	// Create temp destination file
	tempFile, err := os.OpenFile(tempPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0755)
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}
	defer tempFile.Close()

	// Copy the file content
	_, err = io.Copy(tempFile, sourceFile)
	if err != nil {
		return fmt.Errorf("failed to copy file content: %w", err)
	}

	// Ensure all writes are flushed
	err = tempFile.Sync()
	if err != nil {
		return fmt.Errorf("failed to sync temp file: %w", err)
	}

	// Close the temp file before renaming
	tempFile.Close()

	// Set the correct permissions (preserve executable bit)
	err = os.Chmod(tempPath, sourceInfo.Mode())
	if err != nil {
		return fmt.Errorf("failed to set permissions: %w", err)
	}

	// Atomically move temp file to final destination
	err = os.Rename(tempPath, destPath)
	if err != nil {
		return fmt.Errorf("failed to move temp file to destination: %w", err)
	}

	return nil
}
