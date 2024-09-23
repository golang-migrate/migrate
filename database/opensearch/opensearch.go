package opensearch

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/golang-migrate/migrate/v4/database"
	"github.com/opensearch-project/opensearch-go"
)

func init() {
	database.Register("opensearch", &OpenSearch{})
}

const MigrationVersionDocId = "migration_version" // The id of a document used as a lock
const DefaultTimeout = 1 * time.Minute            // Default operations timeout
const DefaultIndex = ".migrations"                // Default index to handle migrations

var (
	ErrInvalidConnStr = errors.New("invalid connection string")
	ErrMissingConfig  = errors.New("missing config")
	ErrInvalidTimeout = errors.New("invalid timeout value")
	ErrInvalidSchema  = errors.New("invalid schema")
	ErrInvalidAction  = errors.New("invalid action format in migration script")
)

// OpenSearch migration driver.
type OpenSearch struct {
	client      *opensearch.Client
	index       string
	lockKey     string
	isLocked    bool
	lockerMutex sync.Mutex
	timeout     time.Duration
}

// OpenSearchCredentials holds credentials for OpenSearch.
type OpenSearchCredentials struct {
	Addresses []string
	Username  string
	Password  string
}

// Config holds the configuration for the OpenSearch driver.
type Config struct {
	Index   string
	Timeout time.Duration
}

type versionDoc struct {
	Version int  `json:"version"`
	Dirty   bool `json:"dirty"`
}

// WithInstance allows the client to provide an instance and migrate using it.
func (d *OpenSearch) WithInstance(instance *opensearch.Client, config *Config) (database.Driver, error) {
	if config == nil {
		return nil, ErrMissingConfig
	}

	if config.Index == "" {
		config.Index = DefaultIndex
	}

	if config.Timeout == 0 {
		config.Timeout = DefaultTimeout
	}

	driver := &OpenSearch{
		client:  instance,
		index:   config.Index,
		lockKey: fmt.Sprintf("%s_lock", config.Index),
		timeout: config.Timeout,
	}

	return driver, nil
}

// Open initializes the driver with the provided connection string.
func (d *OpenSearch) Open(urlStr string) (database.Driver, error) {
	cfg, creds, err := parseConfig(urlStr)
	if err != nil {
		return nil, err
	}

	osCfg := opensearch.Config{
		Addresses: creds.Addresses,
		Username:  creds.Username,
		Password:  creds.Password,
	}

	client, err := opensearch.NewClient(osCfg)
	if err != nil {
		return nil, err
	}

	driver := &OpenSearch{
		client:  client,
		index:   cfg.Index,
		lockKey: fmt.Sprintf("%s_lock", cfg.Index),
		timeout: cfg.Timeout,
	}

	return driver, nil
}

// parseConfig parses the connection string into a Config struct.
func parseConfig(connStr string) (*Config, *OpenSearchCredentials, error) {
	parsedURL, err := url.Parse(connStr)
	if err != nil {
		return nil, nil, ErrInvalidConnStr
	}

	creds := &OpenSearchCredentials{}
	cfg := &Config{Timeout: DefaultTimeout}

	// Scheme validation
	if parsedURL.Scheme != "opensearch" {
		return nil, nil, ErrInvalidSchema
	}

	// User Info
	if parsedURL.User != nil {
		creds.Username = parsedURL.User.Username()
		creds.Password, _ = parsedURL.User.Password()
	}

	// Hosts can be multiple, separated by commas
	hosts := strings.Split(parsedURL.Host, ",")
	for _, host := range hosts {
		if !strings.Contains(host, ":") {
			// Default port 9200
			host = fmt.Sprintf("%s:9200", host)
		}
		creds.Addresses = append(creds.Addresses, fmt.Sprintf("http://%s", host))
	}

	// Path is index name
	if parsedURL.Path != "" && parsedURL.Path != "/" {
		cfg.Index = strings.TrimPrefix(parsedURL.Path, "/")
	} else {
		cfg.Index = DefaultIndex
	}

	// Query Parameters
	q := parsedURL.Query()
	if timeoutStr := q.Get("timeout"); timeoutStr != "" {
		timeout, err := time.ParseDuration(timeoutStr)
		if err != nil {
			return nil, nil, ErrInvalidTimeout
		}
		cfg.Timeout = timeout
	}

	return cfg, creds, nil
}

// Close closes any open resources.
func (d *OpenSearch) Close() error {
	return nil
}

// Lock obtains a lock to prevent concurrent migrations.
func (d *OpenSearch) Lock() error {
	d.lockerMutex.Lock()
	defer d.lockerMutex.Unlock()

	if d.isLocked {
		return database.ErrLocked
	}

	ctx, cancel := context.WithTimeout(context.Background(), d.timeout)
	defer cancel()

	// Try to create the lock document with op_type=create
	lockDoc := map[string]interface{}{
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	}

	lockBody, err := json.Marshal(lockDoc)
	if err != nil {
		return &database.Error{OrigErr: err, Err: "failed to marshal lock document"}
	}

	res, err := d.client.Index(
		d.index,
		bytes.NewReader(lockBody),
		d.client.Index.WithContext(ctx),
		d.client.Index.WithDocumentID(d.lockKey),
		d.client.Index.WithOpType("create"), // Ensures the document is only created if it doesn't exist
	)
	if err != nil {
		return &database.Error{OrigErr: err, Err: "failed to acquire lock"}
	}
	defer func() {
		if err := res.Body.Close(); err != nil {
			fmt.Printf("failed to close response body: %v\n", err)
		}
	}()

	if res.StatusCode == http.StatusCreated {
		// Lock acquired
		d.isLocked = true
		return nil
	} else if res.StatusCode == http.StatusConflict {
		// Lock is held by another process
		return database.ErrLocked
	} else {
		return &database.Error{OrigErr: err, Err: fmt.Sprintf("failed to acquire lock, status: %d", res.StatusCode)}
	}
}

// Unlock releases the migration lock.
func (d *OpenSearch) Unlock() error {
	d.lockerMutex.Lock()
	defer d.lockerMutex.Unlock()

	if !d.isLocked {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), d.timeout)
	defer cancel()

	res, err := d.client.Delete(
		d.index,
		d.lockKey,
		d.client.Delete.WithContext(ctx),
	)
	if err != nil {
		return &database.Error{OrigErr: err, Err: "failed to release lock"}
	}
	defer func() {
		if err := res.Body.Close(); err != nil {
			fmt.Printf("failed to close response body: %v\n", err)
		}
	}()

	if res.StatusCode >= 400 && res.StatusCode != http.StatusNotFound {
		return &database.Error{OrigErr: err, Err: fmt.Sprintf("failed to release lock, status: %d", res.StatusCode)}
	}

	d.isLocked = false
	return nil
}

// Run executes a migration script.
func (d *OpenSearch) Run(migration io.Reader) error {
	var migrationData struct {
		Action string          `json:"action"`
		Body   json.RawMessage `json:"body"`
	}

	decoder := json.NewDecoder(migration)
	if err := decoder.Decode(&migrationData); err != nil {
		return &database.Error{OrigErr: err, Err: "failed to parse migration body"}
	}

	// Parse the action into method and endpoint
	actionParts := strings.Fields(migrationData.Action)
	if len(actionParts) != 2 {
		return ErrInvalidAction
	}
	method, endpoint := strings.ToUpper(actionParts[0]), actionParts[1]

	req, err := http.NewRequest(method, endpoint, bytes.NewReader(migrationData.Body))
	if err != nil {
		return &database.Error{OrigErr: err, Err: "failed to create request"}
	}

	req.Header.Set("Content-Type", "application/json")

	res, err := d.client.Transport.Perform(req)
	if err != nil {
		return &database.Error{OrigErr: err, Err: fmt.Sprintf("request failed with status: %d", res.StatusCode)}
	}
	defer func() {
		if err := res.Body.Close(); err != nil {
			fmt.Printf("failed to close response body: %v\n", err)
		}
	}()

	if res.StatusCode >= 400 {
		resBody, _ := io.ReadAll(res.Body)
		return &database.Error{OrigErr: err, Err: fmt.Sprintf("migration failed with status %d: %s", res.StatusCode, resBody)}
	}

	return nil
}

// Version returns the current migration version.
func (d *OpenSearch) Version() (version int, dirty bool, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), d.timeout)
	defer cancel()

	res, err := d.client.Get(
		d.index,
		MigrationVersionDocId,
		d.client.Get.WithContext(ctx),
	)
	if err != nil {
		return database.NilVersion, false, &database.Error{OrigErr: err, Err: "failed to get migration version"}
	}
	defer func() {
		if err := res.Body.Close(); err != nil {
			fmt.Printf("failed to close response body: %v\n", err)
		}
	}()

	if res.StatusCode == http.StatusNotFound {
		return database.NilVersion, false, nil
	}

	if res.StatusCode >= 400 {
		return database.NilVersion, false, &database.Error{OrigErr: err, Err: fmt.Sprintf("failed to get migration version, status: %d", res.StatusCode)}
	}

	var vdoc struct {
		Source *versionDoc `json:"_source"`
	}
	if err := json.NewDecoder(res.Body).Decode(&vdoc); err != nil {
		return database.NilVersion, false, &database.Error{OrigErr: err, Err: "failed to parse migration version"}
	}

	return vdoc.Source.Version, vdoc.Source.Dirty, nil
}

// SetVersion sets the current migration version.
func (d *OpenSearch) SetVersion(version int, dirty bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), d.timeout)
	defer cancel()

	vdoc := &versionDoc{
		Version: version,
		Dirty:   dirty,
	}

	versionBody, err := json.Marshal(vdoc)
	if err != nil {
		return &database.Error{OrigErr: err, Err: "failed to marshal version document"}
	}

	res, err := d.client.Index(
		d.index,
		bytes.NewReader(versionBody),
		d.client.Index.WithDocumentID(MigrationVersionDocId),
		d.client.Index.WithContext(ctx),
	)
	if err != nil {
		return &database.Error{OrigErr: err, Err: "failed to set migration version"}
	}
	defer func() {
		if err := res.Body.Close(); err != nil {
			fmt.Printf("failed to close response body: %v\n", err)
		}
	}()

	if res.StatusCode >= 400 {
		return &database.Error{OrigErr: err, Err: fmt.Sprintf("failed to set migration version, status: %d", res.StatusCode)}
	}

	return nil
}

// Drop deletes the index related to migrations.
func (d *OpenSearch) Drop() error {
	ctx, cancel := context.WithTimeout(context.Background(), d.timeout)
	defer cancel()

	res, err := d.client.Indices.Delete([]string{d.index}, d.client.Indices.Delete.WithContext(ctx))
	if err != nil {
		return &database.Error{OrigErr: err, Err: "failed to drop index"}
	}
	defer func() {
		if err := res.Body.Close(); err != nil {
			fmt.Printf("failed to close response body: %v\n", err)
		}
	}()

	if res.StatusCode >= 400 {
		resBody, _ := io.ReadAll(res.Body)
		return &database.Error{OrigErr: err, Err: fmt.Sprintf("failed to drop index, status %d: %s", res.StatusCode, resBody)}
	}

	return nil
}
