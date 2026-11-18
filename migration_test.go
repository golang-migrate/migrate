package migrate

import (
	"fmt"
	"io"
	"log"
	"strings"
	"testing"
)

func TestMigrationBufferStripsUTF8BOM(t *testing.T) {
	const migrationBody = "CREATE TABLE users (id INT);"
	migration, err := NewMigration(
		io.NopCloser(strings.NewReader("\xEF\xBB\xBF"+migrationBody)),
		"create_users_table",
		1,
		2,
	)
	if err != nil {
		t.Fatal(err)
	}

	bufferErr := make(chan error, 1)
	go func() {
		bufferErr <- migration.Buffer()
	}()

	buffered, err := io.ReadAll(migration.BufferedBody)
	if err != nil {
		t.Fatal(err)
	}
	if err := <-bufferErr; err != nil {
		t.Fatal(err)
	}

	if got := string(buffered); got != migrationBody {
		t.Fatalf("buffered migration = %q, want %q", got, migrationBody)
	}
	if migration.BytesRead != int64(len(migrationBody)) {
		t.Fatalf("bytes read = %d, want %d", migration.BytesRead, len(migrationBody))
	}
}

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
