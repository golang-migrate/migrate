package bigquery

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	nurl "net/url"
	"strings"
	"time"

	bq "cloud.google.com/go/bigquery"
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
	"github.com/hashicorp/go-multierror"
	uatomic "go.uber.org/atomic"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	bqopt "google.golang.org/api/option"
)

func init() {
	db := BigQuery{}
	database.Register("bigquery", &db)
}

const (
	DefaultMigrationsTableName = "schema_migrations"
	DefaultQueryTimeout        = time.Duration(10) * time.Second
)

const (
	unlockedVal = false
	lockedVal   = true
)

var (
	ErrNoConfig = errors.New("no config")
)

// BigQuery is a database.Driver implementation for running migrations in Big
// Query.
type BigQuery struct {
	DB     *bq.Client
	config *Config
	lock   *uatomic.Bool
}

// Config allows to customize the BigQuery type.
type Config struct {
	MigrationsTable string
	DatasetID       string
	GCPProjectID    string
	StmtTimeout     time.Duration
}

func (c *Config) qualifiedMigrationsTable() string {
	return fmt.Sprintf("%s.%s", c.DatasetID, c.MigrationsTable)
}

// WithInstance is an optional function that accepts an existing DB instance, a
// Config struct and returns a driver instance.
func WithInstance(instance *bq.Client, config *Config) (database.Driver, error) {
	if config == nil {
		return nil, ErrNoConfig
	}

	b := &BigQuery{
		DB:     instance,
		config: config,
		lock:   uatomic.NewBool(unlockedVal),
	}

	err := b.ensureVersionTable()
	if err != nil {
		return nil, err
	}

	return b, nil
}

// Open returns a new driver instance configured with parameters
// coming from the URL string. Migrate will call this function
// only once per instance.
func (b *BigQuery) Open(url string) (database.Driver, error) {
	u, err := nurl.Parse(url)
	if err != nil {
		return nil, err
	}

	projectID, datasetID, err := parseDNS(u)
	if err != nil {
		return nil, err
	}

	migrationsTable := DefaultMigrationsTableName
	if u.Query().Has("x-migrations-table") {
		migrationsTable = u.Query().Get("x-migrations-table")
	}

	var opts []bqopt.ClientOption
	if u.Query().Has("x-endpoint") {
		opts = append(opts, bqopt.WithEndpoint(u.Query().Get("x-endpoint")))
	}

	if u.Query().Has("x-insecure") {
		opts = append(opts, bqopt.WithoutAuthentication())
	}

	if u.Query().Has("x-gcp-credentials-file") {
		opts = append(opts, bqopt.WithCredentialsFile(u.Query().Get("x-gcp-credentials-file")))
	}

	stmtTimeout := DefaultQueryTimeout
	if u.Query().Has("x-stmt-timeout") {
		stmtTimeout, err = time.ParseDuration(u.Query().Get("x-stmt-timeout"))
		if err != nil {
			return nil, err
		}
	}

	ctx := context.Background()
	client, err := bq.NewClient(ctx, projectID, opts...)
	if err != nil {
		return nil, err
	}

	return WithInstance(client, &Config{
		MigrationsTable: migrationsTable,
		DatasetID:       datasetID,
		GCPProjectID:    projectID,
		StmtTimeout:     stmtTimeout,
	})
}

// Close closes the underlying database instance managed by the driver.
// Migrate will call this function only once per instance.
func (b *BigQuery) Close() error {
	return b.DB.Close()
}

// Lock should acquire a database lock so that only one migration process
// can run at a time. Migrate will call this function before Run is called.
// If the implementation can't provide this functionality, return nil.
// Return database.ErrLocked if database is already locked.
func (b *BigQuery) Lock() error {
	if isLocked := b.lock.CAS(unlockedVal, lockedVal); isLocked {
		return nil
	}

	return database.ErrLocked
}

// Unlock should release the lock. Migrate will call this function after
// all migrations have been run.
func (b *BigQuery) Unlock() error {
	if isUnlocked := b.lock.CAS(lockedVal, unlockedVal); isUnlocked {
		return nil
	}

	return database.ErrNotLocked
}

// Run applies a migration to the database. migration is guaranteed to be not nil.
func (b *BigQuery) Run(migration io.Reader) error {
	stmt, err := io.ReadAll(migration)
	if err != nil {
		return &database.Error{OrigErr: err, Err: "migration failed", Query: stmt}
	}

	ctx, cancel := context.WithTimeout(context.Background(), b.config.StmtTimeout)
	defer cancel()

	query := b.DB.Query(string(stmt))

	job, err := query.Run(ctx)
	if err != nil {
		return &database.Error{OrigErr: err, Err: "migration failed", Query: stmt}
	}

	status, err := job.Wait(ctx)
	if err != nil {
		return &database.Error{OrigErr: err, Err: "migration failed", Query: stmt}
	}

	if status.Err() != nil {
		return &database.Error{OrigErr: status.Err(), Err: "migration failed", Query: stmt}
	}

	return nil
}

// SetVersion saves version and dirty state.
// Migrate will call this function before and after each call to Run.
// version must be >= -1. -1 means NilVersion.
func (b *BigQuery) SetVersion(version int, dirty bool) error {
	stmt := fmt.Sprintf(`BEGIN TRANSACTION;
		DELETE FROM`+" `%[1]s` "+`WHERE true;
		INSERT INTO`+" `%[1]s` "+`(version, dirty) VALUES (@version, @dirty);
		COMMIT TRANSACTION;`,
		b.config.qualifiedMigrationsTable(),
	)

	ctx, cancel := context.WithTimeout(context.Background(), b.config.StmtTimeout)
	defer cancel()

	query := b.DB.Query(stmt)
	query.Parameters = []bq.QueryParameter{
		{Name: "version", Value: version},
		{Name: "dirty", Value: dirty},
	}

	job, err := query.Run(ctx)
	if err != nil {
		return &database.Error{OrigErr: err, Err: "failed to set migrations version", Query: []byte(stmt)}
	}

	status, err := job.Wait(ctx)
	if err != nil {
		return &database.Error{OrigErr: err, Err: "failed to set migrations version", Query: []byte(stmt)}
	}

	if status.Err() != nil {
		return &database.Error{OrigErr: status.Err(), Err: "failed to set migrations version", Query: []byte(stmt)}
	}

	return nil
}

// Version returns the currently active version and if the database is dirty.
// When no migration has been applied, it must return version -1.
// Dirty means, a previous migration failed and user interaction is required.
func (b *BigQuery) Version() (version int, dirty bool, err error) {
	stmt := fmt.Sprintf("SELECT version, dirty FROM `%s` ORDER BY version DESC LIMIT 1",
		b.config.qualifiedMigrationsTable(),
	)

	ctx, cancel := context.WithTimeout(context.Background(), b.config.StmtTimeout)
	defer cancel()

	query := b.DB.Query(stmt)

	job, err := query.Run(ctx)
	if err != nil {
		return version, dirty, &database.Error{OrigErr: err, Err: "failed to run migrations version query", Query: []byte(stmt)}
	}

	rowIter, err := job.Read(ctx)
	if err != nil {
		return version, dirty, &database.Error{OrigErr: err, Err: "failed to read migrations version", Query: []byte(stmt)}
	}

	type versionRow struct {
		Version int  `bigquery:"version"`
		Dirty   bool `bigquery:"dirty"`
	}
	v := versionRow{}

	err = rowIter.Next(&v)
	if err != nil {
		if errors.Is(err, iterator.Done) {
			return database.NilVersion, dirty, nil
		}

		return version, dirty, &database.Error{OrigErr: err, Query: []byte(stmt)}
	}

	return v.Version, v.Dirty, nil
}

// Drop deletes everything in the database.
func (b *BigQuery) Drop() error {
	ctx, cancel := context.WithTimeout(context.Background(), b.config.StmtTimeout)
	defer cancel()

	tablesIter := b.DB.Dataset(b.config.DatasetID).Tables(ctx)

	for {
		table, err := tablesIter.Next()
		if err == iterator.Done {
			break
		}

		if err != nil {
			return &database.Error{OrigErr: err, Err: "failed to iterate tables to drop"}
		}

		err = table.Delete(ctx)
		if err != nil {
			return &database.Error{OrigErr: err, Err: "failed to drop tables"}
		}
	}

	return nil
}

// ensureVersionTable creates the migrations version table if it doesn't already
// exist
func (b *BigQuery) ensureVersionTable() (err error) {
	err = b.Lock()
	if err != nil {
		return err
	}

	defer func() {
		unlockErr := b.Unlock()
		if unlockErr != nil {
			if err != nil {
				err = multierror.Append(err, unlockErr)
			}

			err = unlockErr
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), b.config.StmtTimeout)
	defer cancel()

	tableRef := b.DB.Dataset(b.config.DatasetID).Table(b.config.MigrationsTable)
	_, err = tableRef.Metadata(ctx)
	if err == nil {
		// table already exists
		return nil
	}

	if err != nil {
		if e, ok := err.(*googleapi.Error); ok {
			if e.Code != http.StatusNotFound {
				return err
			}
		}
	}

	stmt := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS`+" `%s` "+`(
    version INT64 NOT NULL,
    dirty    BOOL NOT NULL
	)`, b.config.qualifiedMigrationsTable())

	q := b.DB.Query(stmt)

	job, err := q.Run(ctx)
	if err != nil {
		return database.Error{OrigErr: err, Err: "failed to create migrations table"}
	}

	status, err := job.Wait(ctx)
	if err != nil {
		return database.Error{OrigErr: err, Err: "failed to create migrations table"}
	}

	return status.Err()
}

// parseDNS returns the projectID and datasetID fragments from a URL connection
// string like bigquery://{projectID}/{datasetID}?param=true
func parseDNS(u *nurl.URL) (string, string, error) {
	url := strings.Replace(migrate.FilterCustomQuery(u).String(), "bigquery://", "", 1)

	fragments := strings.Split(url, "/")

	if len(fragments) < 2 {
		return "", "", errors.New("invalid url format expected, bigquery://{projectID}/{datasetID}?param=true")
	}

	return fragments[0], strings.TrimSuffix(fragments[1], "?"), nil
}
