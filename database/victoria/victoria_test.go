package victoria

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/golang-migrate/migrate/v4/database"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVictoriaInit(t *testing.T) {
	d := &Victoria{}
	_, err := d.Open("victoria://localhost:8428")
	if err != nil {
		t.Fatal(err)
	}
}

func TestParseConfig(t *testing.T) {
	testCases := []struct {
		name          string
		dsn           string
		expectedError bool
		expectedURL   string
		expectedLabel string
	}{
		{
			name:          "Valid DSN",
			dsn:           "victoria://localhost:8428",
			expectedError: false,
			expectedURL:   "http://localhost:8428",
			expectedLabel: "",
		},
		{
			name:          "Valid DSN with parameters",
			dsn:           "victoria://localhost:8428?label_filter={__name__=\"up\"}&start=2020-01-01T00:00:00Z",
			expectedError: false,
			expectedURL:   "http://localhost:8428",
			expectedLabel: "{__name__=\"up\"}",
		},
		{
			name:          "Invalid scheme",
			dsn:           "postgres://localhost:8428",
			expectedError: true,
		},
		{
			name:          "Invalid URL",
			dsn:           "victoria://",
			expectedError: false,
			expectedURL:   "http://",
			expectedLabel: "",
		},
		{
			name:          "With auth",
			dsn:           "victoria://user:pass@localhost:8428",
			expectedError: false,
			expectedURL:   "http://user:pass@localhost:8428",
			expectedLabel: "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			config, err := parseConfig(tc.dsn)
			if tc.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expectedURL, config.URL)
				assert.Equal(t, tc.expectedLabel, config.LabelFilter)
			}
		})
	}
}

func TestVictoriaLockUnlock(t *testing.T) {
	d := &Victoria{}
	d.isOpen = true

	// Lock should succeed
	err := d.Lock()
	assert.NoError(t, err)
	assert.True(t, d.isLocked)

	// Unlock should succeed
	err = d.Unlock()
	assert.NoError(t, err)
	assert.False(t, d.isLocked)

	// When closed, lock should fail
	d.isOpen = false
	err = d.Lock()
	assert.Equal(t, database.ErrLocked, err)
}

func TestVictoriaClose(t *testing.T) {
	d := &Victoria{
		client: &http.Client{},
		isOpen: true,
	}

	err := d.Close()
	assert.NoError(t, err)
	assert.False(t, d.isOpen)
}

func TestVictoriaRun(t *testing.T) {
	// Setup test server
	testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/import" {
			// Read the request body
			body, _ := io.ReadAll(r.Body)
			defer r.Body.Close()

			// Check if the body contains the expected data
			if !strings.Contains(string(body), "{\"metric\":{\"__name__\":\"test\"}") {
				w.WriteHeader(http.StatusBadRequest)
				return
			}

			// Successful import
			w.WriteHeader(http.StatusNoContent)
		} else {
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer testServer.Close()

	// Parse the URL for our test server
	serverURL := strings.TrimPrefix(testServer.URL, "http://")

	// Create driver and test migration
	d := &Victoria{}
	driver, err := d.Open("victoria://" + serverURL)
	assert.NoError(t, err)

	// Need to lock before running
	err = driver.Lock()
	assert.NoError(t, err)

	// Test with valid migration data
	migrationData := `{"metric":{"__name__":"test","job":"test"},"values":[1],"timestamps":[1596698684000]}`
	err = driver.Run(strings.NewReader(migrationData))
	assert.NoError(t, err)

	// Test with commented lines and empty lines
	migrationData = `
	-- This is a comment
	{"metric":{"__name__":"test","job":"test"},"values":[1],"timestamps":[1596698684000]}
	
	`
	err = driver.Run(strings.NewReader(migrationData))
	assert.NoError(t, err)

	// Test with driver not locked
	driver.Unlock()
	err = driver.Run(strings.NewReader(migrationData))
	assert.Equal(t, database.ErrLocked, err)

	// Test with driver closed
	driver.Lock()
	driver.Close()
	err = driver.Run(strings.NewReader(migrationData))
	assert.Equal(t, database.ErrDatabaseClosed, err)
}

func TestVictoriaVersion(t *testing.T) {
	d := &Victoria{}
	driver, err := d.Open("victoria://localhost:8428")
	assert.NoError(t, err)

	// VictoriaMetrics doesn't support versioning, so we should always get -1
	version, dirty, err := driver.Version()
	assert.NoError(t, err)
	assert.Equal(t, -1, version)
	assert.False(t, dirty)
}

func TestVictoriaSetVersion(t *testing.T) {
	d := &Victoria{}
	driver, err := d.Open("victoria://localhost:8428")
	assert.NoError(t, err)

	// This should be a no-op for VictoriaMetrics
	err = driver.SetVersion(42, true)
	assert.NoError(t, err)
}

func TestVictoriaDrop(t *testing.T) {
	d := &Victoria{}
	driver, err := d.Open("victoria://localhost:8428")
	assert.NoError(t, err)

	// Drop is not supported in VictoriaMetrics
	err = driver.Drop()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not supported")
}
