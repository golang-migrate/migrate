package elasticsearch

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/golang-migrate/migrate/v4/database"
)

func init() {
	driver := new(Elasticsearch)
	database.Register("elasticsearch", driver)
	database.Register("elasticsearch+http", driver)
	database.Register("elasticsearchs", driver)
	database.Register("elasticsearch+https", driver)
}

const (
	// DefaultMigrationsIndex is the index holding the version document.
	DefaultMigrationsIndex = "schema_migrations"
	// DefaultLockIndex is the index used for advisory locking by default.
	DefaultLockIndex = "migrate_advisory_lock"
	// DefaultDropIndicesPattern is the pattern of indices deleted by Drop.
	DefaultDropIndicesPattern = "*"
	// DefaultAdvisoryLockingFlag is the default value of the advisory locking
	// feature flag.
	DefaultAdvisoryLockingFlag = true
	// DefaultLockTimeout is the maximum time to wait for a lock to be released.
	DefaultLockTimeout = 15 * time.Second
	// DefaultLockTimeoutInterval is the maximum interval between two attempts
	// at acquiring the lock.
	DefaultLockTimeoutInterval = 10 * time.Second
	// DefaultRequestTimeout is the timeout of a single request to the cluster.
	DefaultRequestTimeout = 30 * time.Second
)

const (
	// Both the version and the lock document are singletons, so they always
	// live under a fixed id.
	versionDocID = "1"
	lockDocID    = "1"

	errSaveVersion     = "save version failed"
	errGetVersion      = "failed to get migration version"
	errDropIndices     = "drop indices failed"
	errDropDataStreams = "drop data streams failed"
	errMigration       = "migration failed"
)

var (
	ErrNilConfig     = fmt.Errorf("no config")
	ErrNoClusterURL  = fmt.Errorf("no cluster url")
	errLockHeld      = fmt.Errorf("lock is held by another process")
	versionIndexBody = []byte(`{"mappings":{"properties":{"version":{"type":"integer"},"dirty":{"type":"boolean"}}}}`)
	lockIndexBody    = []byte(`{"mappings":{"properties":{"pid":{"type":"integer"},"hostname":{"type":"keyword"},"created_at":{"type":"date"}}}}`)
)

type Locking struct {
	IndexName string
	Timeout   time.Duration
	Interval  time.Duration
	Enabled   bool
}

type Config struct {
	// MigrationsIndex is the index storing the current version.
	MigrationsIndex string
	// DropIndicesPattern is the index pattern deleted by Drop. Elasticsearch
	// has no database to scope migrations to, so this defaults to "*": every
	// non-hidden index of the cluster.
	DropIndicesPattern string
	Locking            Locking
}

type Elasticsearch struct {
	*client
	config   *Config
	isLocked atomic.Bool
}

type versionInfo struct {
	Version int  `json:"version"`
	Dirty   bool `json:"dirty"`
}

// WithInstance returns a driver talking to the cluster at clusterURL through
// the given client. A nil client gets a default one. Credentials, when needed,
// are taken from the userinfo of clusterURL; its query string is ignored, as
// all configuration comes from config.
//
// Note that, unlike Open, this takes Locking.Enabled at face value: advisory
// locking is off unless it is set.
func WithInstance(httpClient *http.Client, clusterURL *url.URL, config *Config) (database.Driver, error) {
	if config == nil {
		return nil, ErrNilConfig
	}
	if clusterURL == nil {
		return nil, ErrNoClusterURL
	}
	if httpClient == nil {
		httpClient = &http.Client{Timeout: DefaultRequestTimeout}
	}
	if len(config.MigrationsIndex) == 0 {
		config.MigrationsIndex = DefaultMigrationsIndex
	}
	if len(config.DropIndicesPattern) == 0 {
		config.DropIndicesPattern = DefaultDropIndicesPattern
	}
	if len(config.Locking.IndexName) == 0 {
		config.Locking.IndexName = DefaultLockIndex
	}
	if config.Locking.Timeout <= 0 {
		config.Locking.Timeout = DefaultLockTimeout
	}
	if config.Locking.Interval <= 0 {
		config.Locking.Interval = DefaultLockTimeoutInterval
	}

	es := &Elasticsearch{
		client: newClient(httpClient, clusterURL),
		config: config,
	}

	if err := es.ping(); err != nil {
		return nil, err
	}
	if config.Locking.Enabled {
		// The lock index is created up front: relying on the cluster to create
		// it on the first write breaks when action.auto_create_index is off.
		if err := es.createIndex(config.Locking.IndexName, lockIndexBody); err != nil {
			return nil, err
		}
	}
	if err := es.ensureVersionTable(); err != nil {
		return nil, err
	}

	return es, nil
}

func (c *Elasticsearch) Open(rawURL string) (database.Driver, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}

	q := u.Query()
	advisoryLocking := DefaultAdvisoryLockingFlag
	if flag := q.Get("x-advisory-locking"); flag != "" {
		advisoryLocking, err = strconv.ParseBool(flag)
		if err != nil {
			return nil, err
		}
	}
	lockTimeout, err := parseSeconds(q.Get("x-advisory-lock-timeout"), DefaultLockTimeout)
	if err != nil {
		return nil, err
	}
	lockInterval, err := parseSeconds(q.Get("x-advisory-lock-timeout-interval"), DefaultLockTimeoutInterval)
	if err != nil {
		return nil, err
	}
	// A migration running _reindex or a snapshot restore can outlast any
	// sensible default, so the timeout is configurable, and 0 disables it.
	requestTimeout, err := parseSeconds(q.Get("x-request-timeout"), DefaultRequestTimeout)
	if err != nil {
		return nil, err
	}

	clusterURL := *u
	switch u.Scheme {
	case "elasticsearch", "elasticsearch+http":
		clusterURL.Scheme = "http"
	case "elasticsearchs", "elasticsearch+https":
		clusterURL.Scheme = "https"
	default:
		return nil, fmt.Errorf("unexpected scheme %q, expected one of elasticsearch, elasticsearch+http, "+
			"elasticsearchs, elasticsearch+https", u.Scheme)
	}

	return WithInstance(&http.Client{Timeout: requestTimeout}, &clusterURL, &Config{
		MigrationsIndex:    q.Get("x-migrations-index"),
		DropIndicesPattern: q.Get("x-drop-indices-pattern"),
		Locking: Locking{
			IndexName: q.Get("x-advisory-lock-index"),
			Timeout:   lockTimeout,
			Interval:  lockInterval,
			Enabled:   advisoryLocking,
		},
	})
}

func (c *Elasticsearch) Close() error {
	c.close()
	return nil
}

// Lock acquires an advisory lock by creating a single document with
// op_type=create, which the cluster rejects with a 409 when it already exists.
func (c *Elasticsearch) Lock() error {
	return database.CasRestoreOnErr(&c.isLocked, false, true, database.ErrLocked, func() error {
		if !c.config.Locking.Enabled {
			return nil
		}

		hostname, err := os.Hostname()
		if err != nil {
			hostname = fmt.Sprintf("Could not determine hostname. Error: %s", err.Error())
		}
		payload, err := json.Marshal(map[string]interface{}{
			"pid":        os.Getpid(),
			"hostname":   hostname,
			"created_at": time.Now().UTC().Format(time.RFC3339),
		})
		if err != nil {
			return err
		}

		p := fmt.Sprintf("/%s/_doc/%s?op_type=create&refresh=true", c.config.Locking.IndexName, lockDocID)
		operation := func() error {
			resp, err := c.do(http.MethodPut, p, payload)
			if err != nil {
				return backoff.Permanent(err)
			}
			switch {
			case resp.ok():
				return nil
			case resp.StatusCode == http.StatusConflict:
				return errLockHeld
			default:
				return backoff.Permanent(resp.err())
			}
		}

		b := backoff.NewExponentialBackOff()
		b.MaxElapsedTime = c.config.Locking.Timeout
		b.MaxInterval = c.config.Locking.Interval

		if err := backoff.Retry(operation, b); err != nil {
			if errors.Is(err, errLockHeld) {
				return database.ErrLocked
			}
			return err
		}
		return nil
	})
}

func (c *Elasticsearch) Unlock() error {
	return database.CasRestoreOnErr(&c.isLocked, true, false, database.ErrNotLocked, func() error {
		if !c.config.Locking.Enabled {
			return nil
		}

		p := fmt.Sprintf("/%s/_doc/%s?refresh=true", c.config.Locking.IndexName, lockDocID)
		resp, err := c.do(http.MethodDelete, p, nil)
		if err != nil {
			return err
		}
		if resp.ok() || resp.StatusCode == http.StatusNotFound {
			return nil
		}
		return resp.err()
	})
}

// Run executes every request of the migration in order. Elasticsearch has no
// transactions, so a request failing halfway through leaves the requests before
// it applied, and the migration is marked dirty.
func (c *Elasticsearch) Run(migration io.Reader) error {
	requests, err := parseRequests(migration)
	if err != nil {
		return err
	}

	for _, r := range requests {
		resp, err := c.send(r.Request)
		if err != nil {
			return &database.Error{OrigErr: err, Err: errMigration, Line: r.Line, Query: []byte(r.String())}
		}
		if !resp.ok() {
			return &database.Error{OrigErr: resp.err(), Err: errMigration, Line: r.Line, Query: []byte(r.String())}
		}
	}
	return nil
}

func (c *Elasticsearch) SetVersion(version int, dirty bool) error {
	payload, err := json.Marshal(versionInfo{Version: version, Dirty: dirty})
	if err != nil {
		return &database.Error{OrigErr: err, Err: errSaveVersion}
	}

	// The write is refreshed so that the version is immediately visible to the
	// next search or get, instead of after the index refresh interval.
	p := fmt.Sprintf("/%s/_doc/%s?refresh=true", c.config.MigrationsIndex, versionDocID)
	resp, err := c.do(http.MethodPut, p, payload)
	if err != nil {
		return &database.Error{OrigErr: err, Err: errSaveVersion}
	}
	if !resp.ok() {
		return &database.Error{OrigErr: resp.err(), Err: errSaveVersion}
	}
	return nil
}

func (c *Elasticsearch) Version() (version int, dirty bool, err error) {
	p := fmt.Sprintf("/%s/_doc/%s", c.config.MigrationsIndex, versionDocID)
	resp, err := c.do(http.MethodGet, p, nil)
	if err != nil {
		return 0, false, &database.Error{OrigErr: err, Err: errGetVersion}
	}
	// Either the index or the document is missing: no migration has run yet.
	if resp.StatusCode == http.StatusNotFound {
		return database.NilVersion, false, nil
	}
	if !resp.ok() {
		return 0, false, &database.Error{OrigErr: resp.err(), Err: errGetVersion}
	}

	var doc struct {
		Found  bool        `json:"found"`
		Source versionInfo `json:"_source"`
	}
	if err := json.Unmarshal(resp.Body, &doc); err != nil {
		return 0, false, &database.Error{OrigErr: err, Err: errGetVersion}
	}
	if !doc.Found {
		return database.NilVersion, false, nil
	}
	return doc.Source.Version, doc.Source.Dirty, nil
}

// Drop deletes every index and data stream matching the configured pattern,
// "*" by default.
func (c *Elasticsearch) Drop() error {
	// Data streams go first. Their backing indices are named with a leading
	// dot, so they are skipped by the index listing, and the cluster only lets
	// them be deleted through the stream that owns them.
	streams, err := c.dataStreams(c.config.DropIndicesPattern)
	if err != nil {
		return err
	}
	if err := c.deleteBatched("/_data_stream/", streams, errDropDataStreams); err != nil {
		return err
	}

	indices, err := c.indices(c.config.DropIndicesPattern)
	if err != nil {
		return err
	}
	return c.deleteBatched("/", indices, errDropIndices)
}

// ensureVersionTable checks if the migrations index exists and, if not,
// creates it. Note that this function locks the database, which deviates from
// the usual convention of "caller locks".
func (c *Elasticsearch) ensureVersionTable() (err error) {
	if err = c.Lock(); err != nil {
		return err
	}
	defer func() {
		if e := c.Unlock(); e != nil {
			err = errors.Join(err, e)
		}
	}()

	return c.createIndex(c.config.MigrationsIndex, versionIndexBody)
}

// parseSeconds parses a url param as a number of seconds, returning
// defaultValue when the param is absent.
func parseSeconds(urlParam string, defaultValue time.Duration) (time.Duration, error) {
	if urlParam == "" {
		return defaultValue, nil
	}
	seconds, err := strconv.Atoi(urlParam)
	if err != nil {
		return 0, err
	}
	if seconds < 0 {
		return 0, fmt.Errorf("expected a number of seconds, got %q", urlParam)
	}
	return time.Duration(seconds) * time.Second, nil
}
