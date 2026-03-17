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
	"github.com/golang-migrate/migrate/v4/source"
)

func init() {
	source.Register("s3", &s3Driver{})
}

type s3Driver struct {
	s3client   s3Client
	config     *Config
	migrations *source.Migrations
}

type s3Client interface {
	ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
}

type Config struct {
	Bucket string
	Prefix string
}

func (s *s3Driver) Open(folder string) (source.Driver, error) {
	parsedConfig, err := parseURI(folder)
	if err != nil {
		return nil, err
	}

	cfg, err := awsconfig.LoadDefaultConfig(context.Background())
	if err != nil {
		return nil, err
	}

	return WithInstance(s3.NewFromConfig(cfg), parsedConfig)
}

func WithInstance(s3client s3Client, config *Config) (source.Driver, error) {
	driver := &s3Driver{
		config:     config,
		s3client:   s3client,
		migrations: source.NewMigrations(),
	}

	if err := driver.loadMigrations(); err != nil {
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

func (s *s3Driver) loadMigrations() error {
	// List all objects in the specified S3 bucket and prefix across all pages.
	var continuationToken *string
	for {
		output, err := s.s3client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
			Bucket:            aws.String(s.config.Bucket),
			Prefix:            aws.String(s.config.Prefix),
			Delimiter:         aws.String("/"),
			ContinuationToken: continuationToken,
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

		if !aws.ToBool(output.IsTruncated) {
			break
		}
		continuationToken = output.NextContinuationToken
		if continuationToken == nil || aws.ToString(continuationToken) == "" {
			return fmt.Errorf("s3 list response was truncated without a continuation token")
		}
	}

	return nil
}

func (s *s3Driver) Close() error {
	return nil
}

func (s *s3Driver) First() (uint, error) {
	v, ok := s.migrations.First()
	if !ok {
		return 0, os.ErrNotExist
	}
	return v, nil
}

func (s *s3Driver) Prev(version uint) (uint, error) {
	v, ok := s.migrations.Prev(version)
	if !ok {
		return 0, os.ErrNotExist
	}
	return v, nil
}

func (s *s3Driver) Next(version uint) (uint, error) {
	v, ok := s.migrations.Next(version)
	if !ok {
		return 0, os.ErrNotExist
	}
	return v, nil
}

func (s *s3Driver) ReadUp(version uint) (io.ReadCloser, string, error) {
	if m, ok := s.migrations.Up(version); ok {
		return s.open(m)
	}
	return nil, "", os.ErrNotExist
}

func (s *s3Driver) ReadDown(version uint) (io.ReadCloser, string, error) {
	if m, ok := s.migrations.Down(version); ok {
		return s.open(m)
	}
	return nil, "", os.ErrNotExist
}

func (s *s3Driver) open(m *source.Migration) (io.ReadCloser, string, error) {
	key := path.Join(s.config.Prefix, m.Raw)
	object, err := s.s3client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(s.config.Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, "", err
	}
	return object.Body, m.Identifier, nil
}
