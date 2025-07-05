package victoria

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/golang-migrate/migrate/v4/database"
)

func init() {
	database.Register("victoria", &Victoria{})
}

// Victoria implements the database.Driver interface for VictoriaMetrics time series database
type Victoria struct {
	client    *http.Client
	url       string
	isLocked  bool
	isOpen    bool
	config    *Config
	importURL string
	exportURL string
}

// Config holds the configuration parameters for VictoriaMetrics connection
type Config struct {
	URL         string
	LabelFilter string
	StartTime   string
	EndTime     string
	Timeout     time.Duration
}

// Open initializes the VictoriaMetrics driver
func (v *Victoria) Open(dsn string) (database.Driver, error) {
	if v.client == nil {
		v.client = &http.Client{
			Timeout: 30 * time.Second,
		}
	}

	config, err := parseConfig(dsn)
	if err != nil {
		return nil, err
	}

	v.config = config
	v.url = config.URL
	v.importURL = config.URL + "/api/v1/import"
	v.exportURL = config.URL + "/api/v1/export"
	v.isOpen = true

	return v, nil
}

// parseConfig parses the DSN into a Config struct
func parseConfig(dsn string) (*Config, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return nil, fmt.Errorf("invalid VictoriaMetrics DSN: %w", err)
	}

	if u.Scheme != "victoria" {
		return nil, fmt.Errorf("invalid scheme for VictoriaMetrics: %s", u.Scheme)
	}

	// Construct the base URL with scheme, host, and port
	baseURL := "http://" + u.Host
	if u.User != nil {
		// Handle authentication if provided
		password, _ := u.User.Password()
		baseURL = "http://" + u.User.Username() + ":" + password + "@" + u.Host
	}

	// Extract query parameters
	timeout := 30 * time.Second
	if timeoutStr := u.Query().Get("timeout"); timeoutStr != "" {
		timeoutVal, err := time.ParseDuration(timeoutStr)
		if err == nil && timeoutVal > 0 {
			timeout = timeoutVal
		}
	}

	return &Config{
		URL:         baseURL,
		LabelFilter: u.Query().Get("label_filter"),
		StartTime:   u.Query().Get("start"),
		EndTime:     u.Query().Get("end"),
		Timeout:     timeout,
	}, nil
}

// Close closes the connection to VictoriaMetrics
func (v *Victoria) Close() error {
	v.isOpen = false
	if v.client != nil {
		v.client.CloseIdleConnections()
	}
	return nil
}

// Lock acquires a database lock (no-op for VictoriaMetrics)
func (v *Victoria) Lock() error {
	if !v.isOpen {
		return database.ErrLocked
	}
	v.isLocked = true
	return nil
}

// Unlock releases a database lock (no-op for VictoriaMetrics)
func (v *Victoria) Unlock() error {
	if !v.isOpen {
		return database.ErrLocked
	}
	v.isLocked = false
	return nil
}

// Run executes a migration by importing data into VictoriaMetrics
func (v *Victoria) Run(migration io.Reader) error {
	if !v.isOpen {
		return database.ErrClosed
	}

	if !v.isLocked {
		return database.ErrLocked
	}

	// Buffer to collect migration data
	var migrationBuffer bytes.Buffer
	
	// Read migration content
	scanner := bufio.NewScanner(migration)
	scanner.Buffer(make([]byte, 4*1024*1024), 4*1024*1024) // 4MB buffer

	for scanner.Scan() {
		line := scanner.Text()
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "--") {
			continue // Skip empty lines and comments
		}
		migrationBuffer.WriteString(line)
		migrationBuffer.WriteString("\n")
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading migration: %w", err)
	}

	// If we have content to import
	if migrationBuffer.Len() > 0 {
		// Send data to VictoriaMetrics
		req, err := http.NewRequest(http.MethodPost, v.importURL, &migrationBuffer)
		if err != nil {
			return fmt.Errorf("failed to create import request: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := v.client.Do(req)
		if err != nil {
			return fmt.Errorf("failed to import data: %w", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
			bodyBytes, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("import failed with status %d: %s", resp.StatusCode, string(bodyBytes))
		}
	}

	return nil
}

// SetVersion sets the migration version (no-op for VictoriaMetrics)
func (v *Victoria) SetVersion(version int, dirty bool) error {
	// VictoriaMetrics doesn't have schema version tracking
	return nil
}

// Version returns the current migration version (no-op for VictoriaMetrics)
func (v *Victoria) Version() (int, bool, error) {
	// VictoriaMetrics doesn't support version tracking
	return -1, false, nil
}

// Drop clears all data (not supported in VictoriaMetrics)
func (v *Victoria) Drop() error {
	return errors.New("drop operation is not supported in VictoriaMetrics")
}

// Ensure Victoria implements the database.Driver interface
var _ database.Driver = (*Victoria)(nil)
var _ database.Locker = (*Victoria)(nil)
