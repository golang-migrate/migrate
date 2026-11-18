package migrate

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"strings"
	"testing"
)

func ExampleNewMigration() {
	// Create a dummy migration body, this is coming from the source usually.
	body := io.NopCloser(strings.NewReader("dumy migration that creates users table"))

	// Create a new Migration that represents version 1486686016.
	// Once this migration has been applied to the database, the new
	// migration version will be 1486689359.
	migr, err := NewMigration(body, "create_users_table", 1486686016, 1486689359)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Print(migr.LogString())
	// Output:
	// 1486686016/u create_users_table
}

func ExampleNewMigration_nilMigration() {
	// Create a new Migration that represents a NilMigration.
	// Once this migration has been applied to the database, the new
	// migration version will be 1486689359.
	migr, err := NewMigration(nil, "", 1486686016, 1486689359)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Print(migr.LogString())
	// Output:
	// 1486686016/u <empty>
}

func ExampleNewMigration_nilVersion() {
	// Create a dummy migration body, this is coming from the source usually.
	body := io.NopCloser(strings.NewReader("dumy migration that deletes users table"))

	// Create a new Migration that represents version 1486686016.
	// This is the last available down migration, so the migration version
	// will be -1, meaning NilVersion once this migration ran.
	migr, err := NewMigration(body, "drop_users_table", 1486686016, -1)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Print(migr.LogString())
	// Output:
	// 1486686016/d drop_users_table
}

// errReader yields prefix and then fails with err. It simulates a source
// (S3, GCS, GitHub, ...) that breaks while the migration body is being read.
type errReader struct {
	prefix []byte
	err    error
	off    int
}

func (r *errReader) Read(p []byte) (int, error) {
	if r.off >= len(r.prefix) {
		return 0, r.err
	}
	n := copy(p, r.prefix[r.off:])
	r.off += n
	return n, nil
}

func (r *errReader) Close() error { return nil }

// TestBufferPropagatesReadError asserts that a failure to read the migration
// body reaches whoever reads BufferedBody, which is the database driver.
// Closing the pipe without the error would signal a clean io.EOF and make the
// driver run a truncated (possibly empty) migration and report success.
func TestBufferPropagatesReadError(t *testing.T) {
	errRead := errors.New("source connection reset")

	tests := []struct {
		name   string
		prefix []byte
	}{
		// Fails inside Buffer's Peek, before anything is written to the pipe.
		{"fails before any data is read", nil},
		{"fails after a partial read", []byte("CREATE TABLE users (")},
		// Larger than the buffer, so Peek succeeds and the read fails later,
		// inside WriteTo, after bytes have already reached the driver.
		{"fails after the buffer is filled", bytes.Repeat([]byte("-- sql\n"), int(DefaultBufferSize)/7+1)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body := &errReader{prefix: tt.prefix, err: errRead}

			migr, err := NewMigration(body, "create_users_table", 1, 2)
			if err != nil {
				t.Fatal(err)
			}

			bufferErr := make(chan error, 1)
			go func() { bufferErr <- migr.Buffer() }()

			// This is what database.Driver.Run does with BufferedBody.
			if _, err := io.ReadAll(migr.BufferedBody); !errors.Is(err, errRead) {
				t.Errorf("reading BufferedBody: got error %v, want %v", err, errRead)
			}

			if err := <-bufferErr; !errors.Is(err, errRead) {
				t.Errorf("Buffer(): got error %v, want %v", err, errRead)
			}
		})
	}
}
