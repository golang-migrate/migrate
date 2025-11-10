package trino

import (
	"testing"

	"github.com/golang-migrate/migrate/v4/database"
)

func TestConfig_Defaults(t *testing.T) {
	config := &Config{}

	// Test default values get set properly
	if config.MigrationsTable != "" {
		t.Errorf("Expected empty MigrationsTable, got %s", config.MigrationsTable)
	}

	if config.MigrationsSchema != "" {
		t.Errorf("Expected empty MigrationsSchema, got %s", config.MigrationsSchema)
	}

	if config.MigrationsCatalog != "" {
		t.Errorf("Expected empty MigrationsCatalog, got %s", config.MigrationsCatalog)
	}
}

func TestTrino_Constants(t *testing.T) {
	if DefaultMigrationsTable != "schema_migrations" {
		t.Errorf("Expected DefaultMigrationsTable to be 'schema_migrations', got %s", DefaultMigrationsTable)
	}
}

func TestTrino_Errors(t *testing.T) {
	if ErrNilConfig == nil {
		t.Error("Expected ErrNilConfig to be defined")
	}
}

func TestTrino_Registration(t *testing.T) {
	// Test that the driver is registered
	drivers := database.List()
	found := false
	for _, driver := range drivers {
		if driver == "trino" {
			found = true
			break
		}
	}

	if !found {
		t.Error("Trino driver should be registered")
	}
}

func TestTrino_DriverInterface(t *testing.T) {
	// Test that Trino implements the database.Driver interface
	var _ database.Driver = &Trino{}
}
