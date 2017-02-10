package migrate

import (
	"fmt"
	"io/ioutil"
	"log"
	"strings"
)

func ExampleNewMigration() {
	// Create a dummy migration body, this is coming from the source usually.
	body := ioutil.NopCloser(strings.NewReader("dumy migration that creates users table"))

	// Create a new Migration that represents version 1486686016.
	// Once this migration has been applied to the database, the new
	// migration version will be 1486689359.
	migr, err := NewMigration(body, "create_users_table", 1486686016, 1486689359)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Print(migr.StringLong())
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

	fmt.Print(migr.StringLong())
	// Output:
	// 1486686016/u <empty>
}
