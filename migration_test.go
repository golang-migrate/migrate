package migrate

import (
	"fmt"
	"io"
	"log"
	"strings"

	"github.com/golang-migrate/migrate/v4/source"
)

func ExampleNewMigration_withBody() {
	// Create a dummy migration body, this is coming from the source usually.
	body := io.NopCloser(strings.NewReader("dumy migration that creates users table"))

	// Create a new Migration that represents version 1486686016.
	// Once this migration has been applied to the database, the new
	// migration version will be 1486689359.
	migr, err := NewMigration(body, nil, "create_users_table", 1486686016, 1486689359)
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
	migr, err := NewMigration(nil, nil, "", 1486686016, 1486689359)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Print(migr.LogString())
	// Output:
	// 1486686016/u <empty>
}

func ExampleNewMigration_withBodyAndNilVersion() {
	// Create a dummy migration body, this is coming from the source usually.
	body := io.NopCloser(strings.NewReader("dumy migration that deletes users table"))

	// Create a new Migration that represents version 1486686016.
	// This is the last available down migration, so the migration version
	// will be -1, meaning NilVersion once this migration ran.
	migr, err := NewMigration(body, nil, "drop_users_table", 1486686016, -1)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Print(migr.LogString())
	// Output:
	// 1486686016/d drop_users_table
}

func ExampleNewMigration_withExecutor() {
	// Create a dummy migration executor.
	e := source.ExecutorFunc(func(_ interface{}) error {
		return nil
	})

	// Create a new Migration that represents version 1486686016.
	// Once this migration has been applied to the database, the new
	// migration version will be 1486689359.
	migr, err := NewMigration(nil, e, "create_users_table", 1486686016, 1486689359)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Print(migr.LogString())
	// Output:
	// 1486686016/u create_users_table
}

func ExampleNewMigration_withExecutorAndNilVersion() {
	// Create a dummy migration executor.
	e := source.ExecutorFunc(func(_ interface{}) error {
		return nil
	})

	// Create a new Migration that represents version 1486686016.
	// This is the last available down migration, so the migration version
	// will be -1, meaning NilVersion once this migration ran.
	migr, err := NewMigration(nil, e, "drop_users_table", 1486686016, -1)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Print(migr.LogString())
	// Output:
	// 1486686016/d drop_users_table
}
