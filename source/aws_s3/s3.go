package awss3

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"go.opentelemetry.io/contrib/instrumentation/github.com/aws/aws-sdk-go-v2/otelaws"

	"github.com/golang-migrate/migrate/v4/source"
)

func init() {
	source.Register("s3", &s3Driver{})
}

// s3APIClient is the subset of the S3 API used by this driver.
type s3APIClient interface {
	ListObjects(ctx context.Context, params *s3.ListObjectsInput, optFns ...func(*s3.Options)) (*s3.ListObjectsOutput, error)
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
}

type s3Driver struct {
	s3client   s3APIClient
	config     *Config
	migrations *source.Migrations
}

type Config struct {
	Bucket string
	Prefix string
}

func (s *s3Driver) Open(ctx context.Context, folder string) (source.Driver, error) {
	config, err := parseURI(folder)
	if err != nil {
		return nil, err
	}

	cfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, err
	}
	otelaws.AppendMiddlewares(&cfg.APIOptions)

	return WithInstance(ctx, s3.NewFromConfig(cfg), config)
}

func WithInstance(ctx context.Context, s3client s3APIClient, config *Config) (source.Driver, error) {
	driver := &s3Driver{
		config:     config,
		s3client:   s3client,
		migrations: source.NewMigrations(),
	}

	if err := driver.loadMigrations(ctx); err != nil {
		return nil, err
	}

	return driver, nil
}

func parseURI(uri string) (*Config, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}

	prefix := strings.Trim(u.Path, "/")
	if prefix != "" {
		prefix += "/"
	}

	return &Config{
		Bucket: u.Host,
		Prefix: prefix,
	}, nil
}

func (s *s3Driver) loadMigrations(ctx context.Context) error {
	output, err := s.s3client.ListObjects(ctx, &s3.ListObjectsInput{
		Bucket:    aws.String(s.config.Bucket),
		Prefix:    aws.String(s.config.Prefix),
		Delimiter: aws.String("/"),
	})
	if err != nil {
		return err
	}
	for _, object := range output.Contents {
		_, fileName := path.Split(aws.ToString(object.Key))
		m, err := source.DefaultParse(fileName)
		if err != nil {
			continue
		}
		if !s.migrations.Append(m) {
			return fmt.Errorf("unable to parse file %v", aws.ToString(object.Key))
		}
	}
	return nil
}

func (s *s3Driver) Close(ctx context.Context) error {
	return nil
}

func (s *s3Driver) First(ctx context.Context) (uint, error) {
	v, ok := s.migrations.First(ctx)
	if !ok {
		return 0, os.ErrNotExist
	}
	return v, nil
}

func (s *s3Driver) Prev(ctx context.Context, version uint) (uint, error) {
	v, ok := s.migrations.Prev(ctx, version)
	if !ok {
		return 0, os.ErrNotExist
	}
	return v, nil
}

func (s *s3Driver) Next(ctx context.Context, version uint) (uint, error) {
	v, ok := s.migrations.Next(ctx, version)
	if !ok {
		return 0, os.ErrNotExist
	}
	return v, nil
}

func (s *s3Driver) ReadUp(ctx context.Context, version uint) (io.ReadCloser, string, error) {
	if m, ok := s.migrations.Up(ctx, version); ok {
		return s.open(ctx, m)
	}
	return nil, "", os.ErrNotExist
}

func (s *s3Driver) ReadDown(ctx context.Context, version uint) (io.ReadCloser, string, error) {
	if m, ok := s.migrations.Down(ctx, version); ok {
		return s.open(ctx, m)
	}
	return nil, "", os.ErrNotExist
}

func (s *s3Driver) open(ctx context.Context, m *source.Migration) (io.ReadCloser, string, error) {
	key := path.Join(s.config.Prefix, m.Raw)
	object, err := s.s3client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.config.Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, "", err
	}
	return object.Body, m.Identifier, nil
}
