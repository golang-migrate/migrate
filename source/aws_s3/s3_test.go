package awss3

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
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
	driver, err := WithInstance(&s3Client, &Config{
		Bucket: "some-bucket",
		Prefix: "prod/migrations/",
	})
	if err != nil {
		t.Fatal(err)
	}
	st.Test(t, driver)
}

func TestLoadMigrationsPaginates(t *testing.T) {
	// A single ListObjects response is capped at 1000 keys by S3. Spread the
	// migrations across several pages (via pageSize) to ensure loadMigrations
	// walks every page instead of silently stopping after the first one.
	const migrationCount = 300
	objects := make(map[string]string, migrationCount*2)
	for i := 1; i <= migrationCount; i++ {
		objects[fmt.Sprintf("prod/migrations/%d_foobar.up.sql", i)] = fmt.Sprintf("%d up", i)
		objects[fmt.Sprintf("prod/migrations/%d_foobar.down.sql", i)] = fmt.Sprintf("%d down", i)
	}
	s3Client := fakeS3{
		bucket:   "some-bucket",
		pageSize: 50,
		objects:  objects,
	}
	driver, err := WithInstance(&s3Client, &Config{
		Bucket: "some-bucket",
		Prefix: "prod/migrations/",
	})
	if err != nil {
		t.Fatal(err)
	}

	first, err := driver.First()
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, uint(1), first)

	version := first
	count := 1
	for {
		next, err := driver.Next(version)
		if errors.Is(err, os.ErrNotExist) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		version = next
		count++
	}
	assert.Equal(t, migrationCount, count, "every migration across all pages should be loaded")
	assert.Equal(t, uint(migrationCount), version, "the highest-numbered migration should be loaded")

	r, identifier, err := driver.ReadUp(uint(migrationCount))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	assert.Equal(t, "foobar", identifier)
	body, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, fmt.Sprintf("%d up", migrationCount), string(body))
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
	s3.S3
	bucket string
	// pageSize caps how many objects each ListObjectsPages page returns so
	// tests can exercise the multi-page path; 0 means a single page.
	pageSize int
	objects  map[string]string
}

func (s *fakeS3) ListObjects(input *s3.ListObjectsInput) (*s3.ListObjectsOutput, error) {
	bucket := aws.StringValue(input.Bucket)
	if bucket != s.bucket {
		return nil, errors.New("bucket not found")
	}
	prefix := aws.StringValue(input.Prefix)
	delimiter := aws.StringValue(input.Delimiter)
	var output s3.ListObjectsOutput
	for name := range s.objects {
		if strings.HasPrefix(name, prefix) {
			if delimiter == "" || !strings.Contains(strings.Replace(name, prefix, "", 1), delimiter) {
				output.Contents = append(output.Contents, &s3.Object{
					Key: aws.String(name),
				})
			}
		}
	}
	return &output, nil
}

func (s *fakeS3) ListObjectsPages(input *s3.ListObjectsInput, fn func(*s3.ListObjectsOutput, bool) bool) error {
	output, err := s.ListObjects(input)
	if err != nil {
		return err
	}
	contents := output.Contents
	pageSize := s.pageSize
	if pageSize <= 0 {
		pageSize = len(contents)
	}
	for start := 0; start < len(contents); start += pageSize {
		end := start + pageSize
		if end > len(contents) {
			end = len(contents)
		}
		lastPage := end == len(contents)
		if !fn(&s3.ListObjectsOutput{Contents: contents[start:end]}, lastPage) {
			break
		}
	}
	return nil
}

func (s *fakeS3) GetObject(input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	bucket := aws.StringValue(input.Bucket)
	if bucket != s.bucket {
		return nil, errors.New("bucket not found")
	}
	if data, ok := s.objects[aws.StringValue(input.Key)]; ok {
		body := io.NopCloser(strings.NewReader(data))
		return &s3.GetObjectOutput{Body: body}, nil
	}
	return nil, errors.New("object not found")
}
