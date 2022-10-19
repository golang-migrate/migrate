package bigquery

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	nurl "net/url"
	"regexp"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"

	"github.com/hashicorp/go-multierror"
	uatomic "go.uber.org/atomic"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

func init() {
	db := BigQuery{}
	database.Register("bigquery", &db)
}

// DefaultMigrationsTable is used if no custom table is specified
const DefaultMigrationsTable = "schema_migrations"

// DefaultLocation is used if no location is specified
const DefaultLocation = "us"

const (
	unlockedVal = 0
	lockedVal   = 1
)

// Driver errors
var (
	ErrNilConfig     = errors.New("no config")
	ErrNoDatasetName = errors.New("no dataset name")
	ErrLockHeld      = errors.New("unable to obtain lock")
	ErrLockNotHeld   = errors.New("unable to release already released lock")
)

// DSN validation
var (
	validDSN = regexp.MustCompile("^projects/(?P<project>[^/]+)/datasets/(?P<dataset>[^/]+)$")
)

// Config used for a BigQuery instance
type Config struct {
	MigrationsTable string
	Location        string
	DatasetName     string
	ProjectID       string
}

// BigQuery implements database.Driver for Google Cloud BigQuery
type BigQuery struct {
	db *DB

	config *Config

	lock *uatomic.Uint32
}

type DB struct {
	client *bigquery.Client
}

func NewDB(client *bigquery.Client) *DB {
	return &DB{
		client: client,
	}
}

// WithInstance implements database.Driver
func WithInstance(instance *DB, config *Config) (database.Driver, error) {
	if config == nil {
		return nil, ErrNilConfig
	}

	if len(config.Location) == 0 {
		config.Location = DefaultLocation
	}

	if len(config.DatasetName) == 0 {
		return nil, ErrNoDatasetName
	}

	if len(config.MigrationsTable) == 0 {
		config.MigrationsTable = DefaultMigrationsTable
	}

	sx := &BigQuery{
		db:     instance,
		config: config,
		lock:   uatomic.NewUint32(unlockedVal),
	}

	if err := sx.ensureVersionTable(); err != nil {
		return nil, err
	}

	return sx, nil
}

// Open implements database.Driver
func (s *BigQuery) Open(url string) (database.Driver, error) {
	purl, err := nurl.Parse(url)
	if err != nil {
		return nil, err
	}

	ctx := context.Background()

	dsn := strings.Replace(migrate.FilterCustomQuery(purl).String(), "bigquery://", "", 1)
	if err = validateDSN(dsn); err != nil {
		return nil, err
	}

	projectID, datasetID, err := parseDSN(dsn)
	if err != nil {
		return nil, err
	}
	bqOpts := []option.ClientOption{}
	endpoint := purl.Query().Get("x-endpoint")
	if endpoint != "" {
		bqOpts = append(bqOpts, option.WithEndpoint(endpoint))
		bqOpts = append(bqOpts, option.WithoutAuthentication())
	}
	client, err := bigquery.NewClient(ctx, projectID, bqOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to bigquery: %v", err)
	}

	migrationsTable := purl.Query().Get("x-migrations-table")

	db := &DB{client: client}
	return WithInstance(db, &Config{
		ProjectID:       projectID,
		DatasetName:     datasetID,
		MigrationsTable: migrationsTable,
	})
}

// Close implements database.Driver
func (s *BigQuery) Close() error {
	return s.db.client.Close()
}

// Lock implements database.Driver but doesn't do anything because BigQuery only
// enqueues the UpdateDatabaseDdlRequest.
func (s *BigQuery) Lock() error {
	if swapped := s.lock.CAS(unlockedVal, lockedVal); swapped {
		return nil
	}
	return ErrLockHeld
}

// Unlock implements database.Driver but no action required, see Lock.
func (s *BigQuery) Unlock() error {
	if swapped := s.lock.CAS(lockedVal, unlockedVal); swapped {
		return nil
	}
	return ErrLockNotHeld
}

// Run implements database.Driver
func (s *BigQuery) Run(migration io.Reader) error {
	migr, err := io.ReadAll(migration)
	if err != nil {
		return err
	}

	ctx := context.Background()
	query := s.asQuery(string(migr))

	job, err := query.Run(ctx)
	if err != nil {
		return &database.Error{OrigErr: err, Err: "migration failed", Query: migr}
	}

	status, err := job.Wait(ctx)
	if err != nil {
		return &database.Error{OrigErr: err, Err: "migration failed", Query: migr}
	}
	if status.Err() != nil {
		return &database.Error{OrigErr: status.Err(), Err: "migration failed", Query: migr}
	}

	return nil
}

type versionStruct struct {
	Version  int   `bigquery:"version"`
	Dirty    bool  `bigquery:"dirty"`
	Sequence int64 `bigquery:"sequence"`
}

// SetVersion implements database.Driver
func (s *BigQuery) SetVersion(version int, dirty bool) error {
	ctx := context.Background()

	migrationTable := s.db.client.Dataset(s.config.DatasetName).Table(s.config.MigrationsTable)

	inserter := migrationTable.Inserter()
	err := inserter.Put(ctx, versionStruct{
		Version:  version,
		Dirty:    dirty,
		Sequence: time.Now().UnixNano(),
	})
	if err != nil {
		return &database.Error{OrigErr: err}
	}

	return nil
}

// Version implements database.Driver
func (s *BigQuery) Version() (version int, dirty bool, err error) {
	ctx := context.Background()

	query := s.asQuery(
		`SELECT version, dirty, sequence FROM ` + s.config.MigrationsTable + ` ORDER BY sequence DESC LIMIT 1`,
	)
	job, err := query.Run(ctx)
	if err != nil {
		return
	}
	iter, err := job.Read(ctx)
	if err != nil {
		return
	}

	v := versionStruct{}
	err = iter.Next(&v)
	switch err {
	case iterator.Done:
		return database.NilVersion, false, nil
	case nil:
		version = v.Version
		dirty = v.Dirty
	default:
		return 0, false, &database.Error{OrigErr: err, Query: []byte(query.Q)}
	}

	return version, dirty, nil
}

// Drop implements database.Driver. Retrieves the database schema first and
// creates statements to drop the indexes and tables accordingly.
// Note: The drop statements are created in reverse order to how they're
// provided in the schema. Assuming the schema describes how the database can
// be "build up", it seems logical to "unbuild" the database simply by going the
// opposite direction. More testing
func (s *BigQuery) Drop() error {
	ctx := context.Background()
	q := s.asQuery(`SELECT table_name as name, ddl FROM INFORMATION_SCHEMA.TABLES`)
	job, err := q.Run(ctx)
	if err != nil {
		return &database.Error{OrigErr: err, Err: "drop failed"}
	}
	iter, err := job.Read(ctx)
	if err != nil {
		return &database.Error{OrigErr: err, Err: "drop failed"}
	}

	v := map[string]bigquery.Value{}
	for {
		err = iter.Next(&v)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return &database.Error{OrigErr: err, Err: "failed iterate on table to delete"}
		}
		name := fmt.Sprintf("%v", v["name"])

		tbl := s.db.client.Dataset(s.config.DatasetName).Table(name)
		_, err = tbl.Metadata(ctx)
		if err != nil {
			if e, ok := err.(*googleapi.Error); ok {
				if e.Code == http.StatusNotFound {
					continue
				}
			}
			return err
		}
		err = tbl.Delete(ctx)
		if err != nil {
			return &database.Error{OrigErr: err, Err: fmt.Sprintf("failed to delete table %s", name)}
		}
	}

	return nil
}

// ensureVersionTable checks if versions table exists and, if not, creates it.
// Note that this function locks the database, which deviates from the usual
// convention of "caller locks" in the BigQuery type.
func (s *BigQuery) ensureVersionTable() (err error) {
	if err = s.Lock(); err != nil {
		return err
	}

	defer func() {
		if e := s.Unlock(); e != nil {
			if err == nil {
				err = e
			} else {
				err = multierror.Append(err, e)
			}
		}
	}()

	ctx := context.Background()

	stmt := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
    version INT64 NOT NULL,
    dirty    BOOL NOT NULL,
	sequence INT64 NOT NULL
	)`, s.config.MigrationsTable)

	q := s.asQuery(stmt)
	job, err := q.Run(ctx)
	if err != nil {
		return &database.Error{OrigErr: err, Query: []byte(stmt)}
	}
	if _, err := job.Wait(ctx); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(stmt)}
	}

	return nil
}

func validateDSN(dsn string) error {
	if matched := validDSN.MatchString(dsn); !matched {
		return fmt.Errorf("dataset uri %q should conform to pattern %q",
			dsn, validDSN.String())
	}
	return nil
}

func parseDSN(dsn string) (project string, dataset string, err error) {
	matches := validDSN.FindStringSubmatch(dsn)
	if len(matches) == 0 {
		return "", "", fmt.Errorf("failed to parse dataset uri from %q according to pattern %q",
			dsn, validDSN.String())
	}
	return matches[1], matches[2], nil
}

func (s *BigQuery) asQuery(stmt string) *bigquery.Query {
	q := s.db.client.Query(stmt)
	q.DefaultDatasetID = s.config.DatasetName
	q.DefaultProjectID = s.config.ProjectID
	return q
}
