package awss3

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	st "github.com/golang-migrate/migrate/v4/source/testing"
	"github.com/stretchr/testify/assert"
)

func Test(t *testing.T) {
	s3Client := fakeS3{
		bucket: "some-bucket",
		objects: map[string]string{
			"staging/migrations/1_foobar.up.sql":          "1 up",
			"staging/migrations/1_foobar.down.sql":        "1 down",
			"prod/migrations/1_foobar.up.sql":             "1 up",
			"prod/migrations/1_foobar.down.sql":           "1 down",
			"prod/migrations/3_foobar.up.sql":             "3 up",
			"prod/migrations/4_foobar.up.sql":             "4 up",
			"prod/migrations/4_foobar.down.sql":           "4 down",
			"prod/migrations/5_foobar.down.sql":           "5 down",
			"prod/migrations/7_foobar.up.sql":             "7 up",
			"prod/migrations/7_foobar.down.sql":           "7 down",
			"prod/migrations/not-a-migration.txt":         "",
			"prod/migrations/0-random-stuff/whatever.txt": "",
		},
	}
	driver, err := WithInstance(context.Background(), &s3Client, &Config{
		Bucket: "some-bucket",
		Prefix: "prod/migrations/",
	})
	if err != nil {
		t.Fatal(err)
	}
	st.Test(t, driver)
}

func TestParseURI(t *testing.T) {
	tests := []struct {
		name   string
		uri    string
		config *Config
	}{
		{
			"with prefix, no trailing slash",
			"s3://migration-bucket/production",
			&Config{
				Bucket: "migration-bucket",
				Prefix: "production/",
			},
		},
		{
			"without prefix, no trailing slash",
			"s3://migration-bucket",
			&Config{
				Bucket: "migration-bucket",
			},
		},
		{
			"with prefix, trailing slash",
			"s3://migration-bucket/production/",
			&Config{
				Bucket: "migration-bucket",
				Prefix: "production/",
			},
		},
		{
			"without prefix, trailing slash",
			"s3://migration-bucket/",
			&Config{
				Bucket: "migration-bucket",
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual, err := parseURI(test.uri)
			if err != nil {
				t.Fatal(err)
			}
			assert.Equal(t, test.config, actual)
		})
	}
}

type fakeS3 struct {
	S3Client
	bucket  string
	objects map[string]string
}

func (s *fakeS3) ListObjects(ctx context.Context, input *s3.ListObjectsInput, optFns ...func(*s3.Options)) (*s3.ListObjectsOutput, error) {
	bucket := stringValue(input.Bucket)
	if bucket != s.bucket {
		return nil, errors.New("bucket not found")
	}
	prefix := stringValue(input.Prefix)
	delimiter := stringValue(input.Delimiter)
	var output s3.ListObjectsOutput
	for name := range s.objects {
		if strings.HasPrefix(name, prefix) {
			if delimiter == "" || !strings.Contains(strings.Replace(name, prefix, "", 1), delimiter) {
				output.Contents = append(output.Contents, s3types.Object{
					Key: aws.String(name),
				})
			}
		}
	}
	return &output, nil
}

func (s *fakeS3) GetObject(ctx context.Context, input *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	bucket := stringValue(input.Bucket)
	if bucket != s.bucket {
		return nil, errors.New("bucket not found")
	}
	if data, ok := s.objects[stringValue(input.Key)]; ok {
		body := io.NopCloser(strings.NewReader(data))
		return &s3.GetObjectOutput{Body: body}, nil
	}
	return nil, errors.New("object not found")
}

func stringValue(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
