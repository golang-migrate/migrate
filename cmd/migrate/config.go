package main

import "github.com/spf13/pflag"

const (
	// configuration defaults support local development (i.e. "go run ...")
	defaultDatabaseDSN      = ""
	defaultDatabaseDriver   = "postgres"
	defaultDatabaseAddress  = "0.0.0.0:5432"
	defaultDatabaseName     = ""
	defaultDatabaseUser     = "postgres"
	defaultDatabasePassword = "postgres"
	defaultDatabaseSSL      = "disable"
	defaultConfigDirectory  = "/cli/config"
)

var (
	// define flag overrides
	flagHelp           = pflag.Bool("help", false, "Print usage")
	flagVersion        = pflag.String("version", Version, "Print version")
	flagLoggingVerbose = pflag.Bool("verbose", true, "Print verbose logging")
	flagPrefetch       = pflag.Uint("prefetch", 10, "Number of migrations to load in advance before executing")
	flaglockTimeout    = pflag.Uint("lock-timeout", 15, "Allow N seconds to acquire database lock")

	flagDatabaseDSN      = pflag.String("database.dsn", defaultDatabaseDSN, "database connection string")
	flagDatabaseDriver   = pflag.String("database.driver", defaultDatabaseDriver, "database driver")
	flagDatabaseAddress  = pflag.String("database.address", defaultDatabaseAddress, "address of the database")
	flagDatabaseName     = pflag.String("database.name", defaultDatabaseName, "name of the database")
	flagDatabaseUser     = pflag.String("database.user", defaultDatabaseUser, "database username")
	flagDatabasePassword = pflag.String("database.password", defaultDatabasePassword, "database password")
	flagDatabaseSSL      = pflag.String("database.ssl", defaultDatabaseSSL, "database ssl mode")

	flagSource = pflag.String("source", "", "Location of the migrations (driver://url)")
	flagPath   = pflag.String("path", "", "Shorthand for -source=file://path")

	flagConfigDirectory = pflag.String("config.source", defaultConfigDirectory, "directory of the configuration file")
	flagConfigFile      = pflag.String("config.file", "", "configuration file name without extension")

	// goto command flags
	flagDirty     = pflag.Bool("force-dirty-handling", false, "force the handling of dirty database state")
	flagMountPath = pflag.String("cache-dir", "", "path to the mounted volume which is used to copy the migration files")
)
