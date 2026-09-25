package awss3

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	st "github.com/golang-migrate/migrate/v4/source/testing"
	"github.com/stretchr/testify/assert"
)

const (
	testBucket          = "some-bucket"
	testMigrationBucket = "migration-bucket"
)

func Test(t *testing.T) {
	s3Client := fakeS3{
		bucket: testBucket,
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
	driver, err := WithInstance(t.Context(), &s3Client, &Config{
		Bucket: testBucket,
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
		bucket:   testBucket,
		pageSize: 50,
		objects:  objects,
	}
	driver, err := WithInstance(t.Context(), &s3Client, &Config{
		Bucket: testBucket,
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
				Bucket: testMigrationBucket,
				Prefix: "production/",
			},
		},
		{
			"without prefix, no trailing slash",
			"s3://migration-bucket",
			&Config{
				Bucket: testMigrationBucket,
			},
		},
		{
			"with prefix, trailing slash",
			"s3://migration-bucket/production/",
			&Config{
				Bucket: testMigrationBucket,
				Prefix: "production/",
			},
		},
		{
			"without prefix, trailing slash",
			"s3://migration-bucket/",
			&Config{
				Bucket: testMigrationBucket,
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
	bucket string
	// pageSize caps how many objects each ListObjects page returns so
	// tests can exercise the multi-page path; 0 means a single page.
	pageSize int
	objects  map[string]string
}

func (s *fakeS3) ListObjects(ctx context.Context, input *s3.ListObjectsInput, optFns ...func(*s3.Options)) (*s3.ListObjectsOutput, error) {
	bucket := aws.ToString(input.Bucket)
	if bucket != s.bucket {
		return nil, fmt.Errorf("bucket %q not found", bucket)
	}
	prefix := aws.ToString(input.Prefix)
	delimiter := aws.ToString(input.Delimiter)
	var names []string
	for name := range s.objects {
		if strings.HasPrefix(name, prefix) {
			if delimiter == "" || !strings.Contains(strings.Replace(name, prefix, "", 1), delimiter) {
				names = append(names, name)
			}
		}
	}
	slices.Sort(names)
	// Marker is the exclusive start key for the page.
	start := 0
	if marker := aws.ToString(input.Marker); marker != "" {
		for i, name := range names {
			if name == marker {
				start = i + 1
				break
			}
		}
	}
	end := len(names)
	if s.pageSize > 0 && end-start > s.pageSize {
		end = start + s.pageSize
	}
	output := &s3.ListObjectsOutput{}
	for _, name := range names[start:end] {
		output.Contents = append(output.Contents, s3types.Object{
			Key: aws.String(name),
		})
	}
	if end < len(names) {
		output.IsTruncated = aws.Bool(true)
		output.NextMarker = aws.String(names[end-1])
	}
	return output, nil
}

func (s *fakeS3) GetObject(ctx context.Context, input *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	bucket := aws.ToString(input.Bucket)
	if bucket != s.bucket {
		return nil, fmt.Errorf("bucket %q not found", bucket)
	}
	objectKey := aws.ToString(input.Key)
	if data, ok := s.objects[objectKey]; ok {
		body := io.NopCloser(strings.NewReader(data))
		return &s3.GetObjectOutput{Body: body}, nil
	}
	return nil, fmt.Errorf("object %q not found", objectKey)
}
