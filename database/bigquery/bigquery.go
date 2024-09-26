package bigquery

import (
	"context"
	"errors"
	"fmt"
	"io"
	nurl "net/url"
	"os"
	"strconv"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/hashicorp/go-multierror"
	"go.uber.org/atomic"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"

	"github.com/golang-migrate/migrate/v4/database"
)

func init() {
	database.Register("bigquery", &BigQuery{})
}

const (
	DefaultMigrationsTable = "schema_migrations"
)

var (
	ErrNoClient    = fmt.Errorf("no client")
	ErrNoDatasetID = fmt.Errorf("no dataset id")
)

type Config struct {
	MigrationsTable  string
	StatementTimeout time.Duration
	DatasetID        string
}

type VersionInfo struct {
	Version int  `bigquery:"version"`
	Dirty   bool `bigquery:"dirty"`
}

type BigQuery struct {
	client *bigquery.Client

	// Locking and unlocking need to use the same connection
	isLocked atomic.Bool

	// Open and WithInstance need to guarantee that config is never nil
	config *Config
}

func WithInstance(ctx context.Context, client *bigquery.Client, config *Config) (database.Driver, error) {
	if client == nil {
		return nil, ErrNoClient
	}

	if config == nil {
		config = &Config{}
	}

	job, err := client.Query("SELECT 1").Run(ctx)
	if err != nil {
		return nil, err
	}

	_, err = job.Read(ctx)
	if err != nil {
		return nil, err
	}

	if len(config.DatasetID) == 0 {
		return nil, ErrNoDatasetID
	}

	if len(config.MigrationsTable) == 0 {
		config.MigrationsTable = DefaultMigrationsTable
	}

	bx := &BigQuery{
		client: client,
		config: config,
	}

	if err := bx.ensureVersionTable(); err != nil {
		return nil, err
	}

	return bx, nil
}

func (b *BigQuery) Open(url string) (database.Driver, error) {
	ctx := context.Background()

	purl, err := nurl.Parse(url)
	if err != nil {
		return nil, err
	}

	config := &Config{}

	opts := make([]option.ClientOption, 0)

	q := purl.Query()

	if q.Has("x-migrations-table") {
		config.MigrationsTable = q.Get("x-migrations-table")
	}

	if q.Has("x-statement-timeout") {
		statementTimeoutString := q.Get("x-statement-timeout")
		if statementTimeoutString != "" {
			statementTimeout, err := strconv.Atoi(statementTimeoutString)
			if err != nil {
				return nil, err
			}
			config.StatementTimeout = time.Duration(statementTimeout)
		}
	}

	if q.Has("credentials_filename") {
		opts = append(opts, option.WithCredentialsFile(q.Get("credentials_filename")))
	} else if os.Getenv("GOOGLE_APPLICATION_CREDENTIALS") != "" {
		opts = append(opts, option.WithCredentialsFile(os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")))
	} else {
		opts = append(opts, option.WithoutAuthentication())
	}

	projectID := bigquery.DetectProjectID
	if q.Has("project_id") {
		projectID = q.Get("project_id")
	}

	if q.Has("dataset_id") {
		config.DatasetID = q.Get("dataset_id")
	}

	opts = append(opts, option.WithEndpoint(fmt.Sprintf("%s%s", purl.Host, purl.Path)))

	client, err := bigquery.NewClient(ctx, projectID, opts...)
	if err != nil {
		return nil, err
	}

	bx, err := WithInstance(ctx, client, config)
	if err != nil {
		return nil, err
	}

	return bx, nil
}

func (b *BigQuery) Close() error {
	err := b.client.Close()
	if err != nil {
		return err
	}

	return nil
}

func (b *BigQuery) Lock() error {
	if !b.isLocked.CAS(false, true) {
		return database.ErrLocked
	}
	return nil
}

func (b *BigQuery) Unlock() error {
	if !b.isLocked.CAS(true, false) {
		return database.ErrNotLocked
	}
	return nil
}

func (b *BigQuery) Run(migration io.Reader) error {
	migr, err := io.ReadAll(migration)
	if err != nil {
		return fmt.Errorf("error on Run: %w", err)
	}

	statement := migr[:]

	ctx := context.Background()
	if b.config.StatementTimeout != 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, b.config.StatementTimeout)
		defer cancel()
	}

	query := b.client.Query(string(statement))
	query.DefaultDatasetID = b.config.DatasetID

	job, err := query.Run(ctx)
	if err != nil {
		return fmt.Errorf("error on Run: %w", err)
	}

	_, err = job.Read(ctx)
	if err != nil {
		var gErr *googleapi.Error
		if errors.As(err, &gErr) {
			return fmt.Errorf("error on Run: %w\n%s", gErr, string(statement))
		}
		return fmt.Errorf("error on Run: %w", err)
	}

	return nil
}

func (b *BigQuery) SetVersion(version int, dirty bool) error {
	ctx := context.Background()

	query := fmt.Sprintf(`
		BEGIN TRANSACTION;
		DELETE FROM `+"`%[1]s.%[2]s`"+` WHERE true; 
		INSERT INTO `+"`%[1]s.%[2]s`"+` (version, dirty) VALUES (%[3]d, %[4]t);
		COMMIT TRANSACTION;
	`, b.config.DatasetID, b.config.MigrationsTable, version, dirty)

	job, err := b.client.Query(query).Run(ctx)
	if err != nil {
		return fmt.Errorf("error on SetVersion: %w", err)
	}

	_, err = job.Read(ctx)
	if err != nil {
		return fmt.Errorf("error on SetVersion: %w", err)
	}

	return nil
}

func (b *BigQuery) Version() (int, bool, error) {
	ctx := context.Background()

	it := b.getVersionTable().Read(ctx)

	versionInfo := VersionInfo{}
	if err := it.Next(&versionInfo); err != nil {
		if err.Error() != "no more items in iterator" {
			return database.NilVersion, false, fmt.Errorf("error on Version: %w", err)
		}
		return database.NilVersion, false, nil
	}

	return versionInfo.Version, versionInfo.Dirty, nil
}

func (b *BigQuery) Drop() error {
	ctx := context.Background()

	it := b.getDataset().Tables(ctx)

	for {
		table, err := it.Next()
		if err != nil {
			if err.Error() == "no more items in iterator" {
				break
			}
			return fmt.Errorf("error on Drop: %w", err)
		}

		err = table.Delete(ctx)
		if err != nil {
			return fmt.Errorf("error on Drop: %w", err)
		}
	}

	return nil
}

// ensureVersionTable checks if versions table exists and, if not, creates it.
func (b *BigQuery) ensureVersionTable() (err error) {
	if err = b.Lock(); err != nil {
		return err
	}

	defer func() {
		if e := b.Unlock(); e != nil {
			if err == nil {
				err = e
			} else {
				err = multierror.Append(err, e)
			}
		}
	}()

	ctx := context.Background()

	table := b.getVersionTable()

	// This block checks whether the `MigrationsTable` already exists.
	// This is useful because it allows read only users to also check the current version.
	md, err := table.Metadata(ctx)
	if err != nil {
		var gErr *googleapi.Error
		if !errors.As(err, &gErr) || gErr.Code != 404 {
			return err
		}
	}

	if md != nil {
		return nil
	}

	schema, err := bigquery.InferSchema(VersionInfo{})
	if err != nil {
		return err
	}

	md = &bigquery.TableMetadata{Schema: schema}
	if err := table.Create(ctx, md); err != nil {
		return err
	}

	return nil
}

func (b *BigQuery) getDataset() *bigquery.Dataset {
	return b.client.Dataset(b.config.DatasetID)
}

func (b *BigQuery) getVersionTable() *bigquery.Table {
	return b.getDataset().Table(b.config.MigrationsTable)
}
