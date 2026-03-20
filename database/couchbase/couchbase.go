package couchbase

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/couchbase/gocb/v2"
	"github.com/golang-migrate/migrate/v4/database"
)

func init() {
	db := Couchbase{}
	database.Register("couchbase", &db)
	database.Register("couchbases", &db) // TLS variant
}

const (
	DefaultScopeName            = "_default"
	DefaultMigrationsCollection = "migrations"
	DefaultLockingCollection    = "migrate_advisory_lock"
	DefaultLockTimeout          = 15
	DefaultLockTimeoutInterval  = 10
	DefaultAdvisoryLockingFlag  = true
	lockDocID                   = "migrate_lock"
	versionDocID                = "migrate_version"
	contextWaitTimeout          = 5 * time.Second
	startupWaitTimeout          = 30 * time.Second
)

var (
	ErrNilConfig    = fmt.Errorf("no config")
	ErrNoBucketName = fmt.Errorf("no bucket name")
	ErrNoScopeName  = fmt.Errorf("no scope name")
)

type Locking struct {
	CollectionName string
	Timeout        int
	Enabled        bool
	Interval       int
}

type Config struct {
	BucketName           string
	ScopeName            string
	MigrationsCollection string
	Locking              Locking
}

type Couchbase struct {
	cluster  *gocb.Cluster
	bucket   *gocb.Bucket
	scope    *gocb.Scope
	config   *Config
	isLocked atomic.Bool
}

type versionInfo struct {
	Version int  `json:"version"`
	Dirty   bool `json:"dirty"`
}

// N1QLMigration represents a single N1QL statement to execute.
type N1QLMigration struct {
	Query  string `json:"query"`
	Params []any  `json:"params,omitempty"`
}

func WithInstance(cluster *gocb.Cluster, config *Config) (database.Driver, error) {
	if config == nil {
		return nil, ErrNilConfig
	}
	if len(config.BucketName) == 0 {
		return nil, ErrNoBucketName
	}
	if len(config.ScopeName) == 0 {
		config.ScopeName = DefaultScopeName
	}
	if len(config.MigrationsCollection) == 0 {
		config.MigrationsCollection = DefaultMigrationsCollection
	}
	if len(config.Locking.CollectionName) == 0 {
		config.Locking.CollectionName = DefaultLockingCollection
	}
	if config.Locking.Timeout <= 0 {
		config.Locking.Timeout = DefaultLockTimeout
	}
	if config.Locking.Interval <= 0 {
		config.Locking.Interval = DefaultLockTimeoutInterval
	}

	bucket := cluster.Bucket(config.BucketName)
	if err := bucket.WaitUntilReady(startupWaitTimeout, nil); err != nil {
		return nil, fmt.Errorf("bucket not ready: %w", err)
	}

	cb := &Couchbase{
		cluster: cluster,
		bucket:  bucket,
		scope:   bucket.Scope(config.ScopeName),
		config:  config,
	}

	if err := cb.ensureCollections(); err != nil {
		return nil, err
	}
	if err := cb.ensureVersionDoc(); err != nil {
		return nil, err
	}

	return cb, nil
}

func (cb *Couchbase) Open(dsn string) (database.Driver, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}

	// Connection string: couchbase://host/bucket?x-scope=scopeName&...
	bucketName := u.Path
	if len(bucketName) > 0 && bucketName[0] == '/' {
		bucketName = bucketName[1:]
	}
	if len(bucketName) == 0 {
		return nil, ErrNoBucketName
	}

	q := u.Query()

	// Extract custom parameters before passing to gocb
	scopeName := q.Get("x-scope")
	migrationsCollection := q.Get("x-migrations-collection")
	lockCollection := q.Get("x-advisory-lock-collection")

	advisoryLockingFlag, err := parseBoolean(q.Get("x-advisory-locking"), DefaultAdvisoryLockingFlag)
	if err != nil {
		return nil, err
	}
	lockTimeout, err := parseInt(q.Get("x-advisory-lock-timeout"), DefaultLockTimeout)
	if err != nil {
		return nil, err
	}
	lockInterval, err := parseInt(q.Get("x-advisory-lock-timeout-interval"), DefaultLockTimeoutInterval)
	if err != nil {
		return nil, err
	}

	// Extract credentials from URL
	username := u.User.Username()
	password, _ := u.User.Password()

	// Build the couchbase connection string without custom params
	for _, key := range []string{
		"x-scope", "x-migrations-collection", "x-advisory-lock-collection",
		"x-advisory-locking", "x-advisory-lock-timeout", "x-advisory-lock-timeout-interval",
	} {
		q.Del(key)
	}

	connStr := fmt.Sprintf("%s://%s", u.Scheme, u.Host)
	if len(q) > 0 {
		connStr += "?" + q.Encode()
	}

	opts := gocb.ClusterOptions{}
	if username != "" || password != "" {
		opts.Authenticator = gocb.PasswordAuthenticator{
			Username: username,
			Password: password,
		}
	}

	cluster, err := gocb.Connect(connStr, opts)
	if err != nil {
		return nil, err
	}

	if err := cluster.WaitUntilReady(startupWaitTimeout, nil); err != nil {
		return nil, fmt.Errorf("cluster not ready: %w", err)
	}

	return WithInstance(cluster, &Config{
		BucketName:           bucketName,
		ScopeName:            scopeName,
		MigrationsCollection: migrationsCollection,
		Locking: Locking{
			CollectionName: lockCollection,
			Timeout:        lockTimeout,
			Enabled:        advisoryLockingFlag,
			Interval:       lockInterval,
		},
	})
}

func (cb *Couchbase) Close() error {
	return cb.cluster.Close(nil)
}

func (cb *Couchbase) Lock() error {
	return database.CasRestoreOnErr(&cb.isLocked, false, true, database.ErrLocked, func() error {
		if !cb.config.Locking.Enabled {
			return nil
		}

		col := cb.scope.Collection(cb.config.Locking.CollectionName)

		deadline := time.Now().Add(time.Duration(cb.config.Locking.Timeout) * time.Second)
		interval := 100 * time.Millisecond
		maxInterval := time.Duration(cb.config.Locking.Interval) * time.Second

		for {
			// Try to insert lock document; fails if it already exists
			_, err := col.Insert(lockDocID, map[string]any{
				"locked_at": time.Now().UTC().Format(time.RFC3339),
				"pid":       fmt.Sprintf("%d", time.Now().UnixNano()),
			}, &gocb.InsertOptions{
				Timeout: contextWaitTimeout,
			})
			if err == nil {
				return nil
			}

			// If the document already exists, the lock is held
			if !errors.Is(err, gocb.ErrDocumentExists) {
				return database.ErrLocked
			}

			if time.Now().After(deadline) {
				return database.ErrLocked
			}

			time.Sleep(interval)
			// Exponential backoff
			interval = min(interval*2, maxInterval)
		}
	})
}

func (cb *Couchbase) Unlock() error {
	return database.CasRestoreOnErr(&cb.isLocked, true, false, database.ErrNotLocked, func() error {
		if !cb.config.Locking.Enabled {
			return nil
		}

		col := cb.scope.Collection(cb.config.Locking.CollectionName)
		_, err := col.Remove(lockDocID, &gocb.RemoveOptions{
			Timeout: contextWaitTimeout,
		})
		if err != nil && !errors.Is(err, gocb.ErrDocumentNotFound) {
			return err
		}
		return nil
	})
}

// Run executes a migration. Migrations are JSON arrays of N1QL statements:
//
//	[{"query": "CREATE INDEX ..."},{"query": "INSERT INTO ...","params": [1, "val"]}]
func (cb *Couchbase) Run(migration io.Reader) error {
	data, err := io.ReadAll(migration)
	if err != nil {
		return err
	}

	var stmts []N1QLMigration
	if err := json.Unmarshal(data, &stmts); err != nil {
		return fmt.Errorf("unmarshaling migration json: %w", err)
	}

	for _, stmt := range stmts {
		if len(stmt.Query) == 0 {
			continue
		}

		opts := &gocb.QueryOptions{
			Timeout: 60 * time.Second,
		}
		if len(stmt.Params) > 0 {
			opts.PositionalParameters = stmt.Params
		}

		_, err := cb.cluster.Query(stmt.Query, opts)
		if err != nil {
			return &database.Error{
				OrigErr: err,
				Query:   []byte(stmt.Query),
				Err:     "migration failed",
			}
		}
	}

	return nil
}

func (cb *Couchbase) SetVersion(version int, dirty bool) error {
	col := cb.scope.Collection(cb.config.MigrationsCollection)

	doc := versionInfo{
		Version: version,
		Dirty:   dirty,
	}

	_, err := col.Upsert(versionDocID, doc, &gocb.UpsertOptions{
		Timeout: contextWaitTimeout,
	})
	if err != nil {
		return &database.Error{OrigErr: err, Err: "save version failed"}
	}
	return nil
}

func (cb *Couchbase) Version() (version int, dirty bool, err error) {
	col := cb.scope.Collection(cb.config.MigrationsCollection)

	result, err := col.Get(versionDocID, &gocb.GetOptions{
		Timeout: contextWaitTimeout,
	})
	if err != nil {
		if errors.Is(err, gocb.ErrDocumentNotFound) {
			return database.NilVersion, false, nil
		}
		return 0, false, &database.Error{OrigErr: err, Err: "failed to get migration version"}
	}

	var info versionInfo
	if err := result.Content(&info); err != nil {
		return 0, false, &database.Error{OrigErr: err, Err: "failed to decode migration version"}
	}

	return info.Version, info.Dirty, nil
}

func (cb *Couchbase) Drop() error {
	mgr := cb.bucket.Collections()

	// Drop all collections in the configured scope (except system collections)
	scopes, err := mgr.GetAllScopes(nil)
	if err != nil {
		return &database.Error{OrigErr: err, Err: "failed to list scopes"}
	}

	for _, scope := range scopes {
		if scope.Name != cb.config.ScopeName {
			continue
		}
		for _, col := range scope.Collections {
			if col.Name == DefaultScopeName {
				// Flush default collection documents via N1QL
				_, qErr := cb.cluster.Query(
					fmt.Sprintf("DELETE FROM `%s`.`%s`.`%s`", cb.config.BucketName, cb.config.ScopeName, col.Name),
					nil,
				)
				if qErr != nil {
					return &database.Error{OrigErr: qErr, Err: "failed to flush default collection"}
				}
				continue
			}
			err := mgr.DropCollection(gocb.CollectionSpec{
				Name:      col.Name,
				ScopeName: cb.config.ScopeName,
			}, nil)
			if err != nil {
				return &database.Error{OrigErr: err, Err: fmt.Sprintf("failed to drop collection %s", col.Name)}
			}
		}
	}

	return nil
}

// ensureCollections creates the migrations and locking collections if they don't exist.
func (cb *Couchbase) ensureCollections() error {
	mgr := cb.bucket.Collections()

	// Ensure scope exists (skip _default)
	if cb.config.ScopeName != DefaultScopeName {
		_ = mgr.CreateScope(cb.config.ScopeName, nil) // ignore already-exists errors
	}

	// Ensure migrations collection
	err := mgr.CreateCollection(gocb.CollectionSpec{
		Name:      cb.config.MigrationsCollection,
		ScopeName: cb.config.ScopeName,
	}, nil)
	if err != nil && !errors.Is(err, gocb.ErrCollectionExists) {
		return fmt.Errorf("create migrations collection: %w", err)
	}

	// Ensure locking collection
	if cb.config.Locking.Enabled {
		err = mgr.CreateCollection(gocb.CollectionSpec{
			Name:      cb.config.Locking.CollectionName,
			ScopeName: cb.config.ScopeName,
		}, nil)
		if err != nil && !errors.Is(err, gocb.ErrCollectionExists) {
			return fmt.Errorf("create locking collection: %w", err)
		}
	}

	// Wait for collections to be ready
	time.Sleep(500 * time.Millisecond)

	return nil
}

// ensureVersionDoc verifies that the version document is accessible.
// Note that this function locks the database, which deviates from the usual
// convention of "caller locks" in the Couchbase type.
func (cb *Couchbase) ensureVersionDoc() (err error) {
	if err = cb.Lock(); err != nil {
		return err
	}

	defer func() {
		if e := cb.Unlock(); e != nil {
			err = errors.Join(err, e)
		}
	}()

	if _, _, err = cb.Version(); err != nil {
		return err
	}
	return nil
}

func parseBoolean(urlParam string, defaultValue bool) (bool, error) {
	if urlParam != "" {
		return strconv.ParseBool(urlParam)
	}
	return defaultValue, nil
}

func parseInt(urlParam string, defaultValue int) (int, error) {
	if urlParam != "" {
		result, err := strconv.Atoi(urlParam)
		if err != nil {
			return -1, err
		}
		return result, nil
	}
	return defaultValue, nil
}

// Ensure we satisfy the database.Driver interface at compile time.
var _ database.Driver = (*Couchbase)(nil)
