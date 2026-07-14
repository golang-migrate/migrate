package victoria

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestVictoriaExport(t *testing.T) {
	// Sample export data
	exportData := `{"metric":{"__name__":"up","job":"test"},"values":[1],"timestamps":[1596698684000]}
{"metric":{"__name__":"cpu_usage","instance":"server1"},"values":[0.45],"timestamps":[1596698684000]}`

	// Setup test server
	testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/export" {
			// Check query parameters
			query := r.URL.Query()
			assert.Equal(t, "{__name__=\"up\"}", query.Get("match[]"))
			assert.Equal(t, "2020-01-01T00:00:00Z", query.Get("start"))
			assert.Equal(t, "2020-01-02T00:00:00Z", query.Get("end"))

			// Return export data
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(exportData))
		} else {
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer testServer.Close()

	// Parse the URL for our test server
	serverURL := strings.TrimPrefix(testServer.URL, "http://")

	// Create driver
	d := &Victoria{}
	dsn := "victoria://" + serverURL + "?label_filter={__name__=\"up\"}&start=2020-01-01T00:00:00Z&end=2020-01-02T00:00:00Z"
	// No need to store the returned driver since we're testing the receiver methods directly
	_, err := d.Open(dsn)
	assert.NoError(t, err)

	// Test export
	var buf bytes.Buffer
	err = d.Export(context.Background(), &buf)
	assert.NoError(t, err)
	assert.Equal(t, exportData, buf.String())

	// Test export with closed connection
	d.Close()
	err = d.Export(context.Background(), &buf)
	assert.Error(t, err)
	assert.Equal(t, ErrClosed, err)
}

func TestVictoriaExportError(t *testing.T) {
	// Setup test server that returns an error
	testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Internal server error"))
	}))
	defer testServer.Close()

	// Parse the URL for our test server
	serverURL := strings.TrimPrefix(testServer.URL, "http://")

	// Create driver
	d := &Victoria{}
	dsn := "victoria://" + serverURL
	_, err := d.Open(dsn)
	assert.NoError(t, err)

	// Test export
	var buf bytes.Buffer
	err = d.Export(context.Background(), &buf)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "status 500")
}
